package distpow

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type WorkerAddr string

type WorkerClient struct {
	addr       WorkerAddr
	client     *rpc.Client
	workerByte uint8
}

type CoordinatorConfig struct {
	ClientAPIListenAddr string
	WorkerAPIListenAddr string
	Workers             []WorkerAddr
	TracerServerAddr    string
	TracerSecret        []byte
}

type CoordinatorMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CoordinatorWorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorWorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
}

type CoordinatorWorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorSuccess struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type Coordinator struct {
	config  CoordinatorConfig
	tracer  *tracing.Tracer
	workers []*WorkerClient
}

/****** RPC structs ******/
type CoordMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Token            tracing.TracingToken
}

type CoordMineResponse struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
	Token            tracing.TracingToken
}

type CoordResultArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	Token            tracing.TracingToken
}

type ResultChan chan CoordResultArgs

type Secret []uint8

type MineTask struct {
	Wg            *sync.WaitGroup
	Mu            *sync.Mutex
	Secret        Secret
	IsFirstResult bool
}

type CoordRPCHandler struct {
	tracer     *tracing.Tracer
	workers    []*WorkerClient
	workerBits uint
	tasks      TasksCache
	cache      Cache
}

type NonceCache struct {
	maxNumTrailingZeros uint
	secret              Secret
}

type Cache struct {
	mu         sync.Mutex
	nonceCache map[string]NonceCache
}

type TasksCache struct {
	mu    sync.Mutex
	tasks map[string]*MineTask
}

type Reply struct {
	Token tracing.TracingToken
}

func NewCoordinator(config CoordinatorConfig) *Coordinator {
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: "coordinator",
		Secret:         config.TracerSecret,
	})

	workerClients := make([]*WorkerClient, len(config.Workers))
	for i, addr := range config.Workers {
		workerClients[i] = &WorkerClient{
			addr:       addr,
			client:     nil,
			workerByte: uint8(i),
		}
	}

	return &Coordinator{
		config:  config,
		tracer:  tracer,
		workers: workerClients,
	}
}

// Mine is a blocking RPC from powlib instructing the Coordinator to solve a specific pow instance
func (c *CoordRPCHandler) Mine(args CoordMineArgs, reply *CoordMineResponse) error {
	trace := c.tracer.ReceiveToken(args.Token)

	trace.RecordAction(CoordinatorMine{
		NumTrailingZeros: args.NumTrailingZeros,
		Nonce:            args.Nonce,
	})

	if secret, exists := c.cache.getSecretIfExists(args.Nonce, args.NumTrailingZeros, trace); exists {
		return c.mineComplete(
			reply,
			CoordinatorSuccess{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				Secret:           secret,
			},
			trace)
	}

	taskKey := generateCoordTaskKey(args.Nonce, args.NumTrailingZeros)
	task := c.tasks.createTask(taskKey)

	// initialize and connect to workers (if not already connected)
	for err := initializeWorkers(c.workers); err != nil; {
		log.Println(err)
		err = initializeWorkers(c.workers)
	}

	workerCount := len(c.workers)

	task.Wg.Add(len(c.workers))
	var calleeReply *Reply
	for _, w := range c.workers {
		trace.RecordAction(CoordinatorWorkerMine{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
		})
		args := WorkerMineArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
			WorkerBits:       c.workerBits,
			Token:            trace.GenerateToken(),
		}
		err := w.client.Call("WorkerRPCHandler.Mine", args, &calleeReply)
		if err != nil {
			return err
		}
		c.tracer.ReceiveToken(calleeReply.Token)
	}

	log.Printf("Waiting for %d acks from workers, then we are done", workerCount)

	task.Wg.Wait()

	if secret, exists := c.cache.getSecretIfExists(args.Nonce, args.NumTrailingZeros, trace); exists {
		c.tasks.deleteTask(taskKey)
		return c.mineComplete(
			reply,
			CoordinatorSuccess{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				Secret:           secret,
			},
			trace)
	}

	// deleteTask completed mine task from map
	c.tasks.deleteTask(taskKey)
	return fmt.Errorf("Task did not contain a secret: %s", taskKey)
}

func (c *CoordRPCHandler) mineComplete(reply *CoordMineResponse, success CoordinatorSuccess, trace *tracing.Trace) error {
	reply.NumTrailingZeros = success.NumTrailingZeros
	reply.Nonce = success.Nonce
	reply.Secret = success.Secret

	trace.RecordAction(success)
	reply.Token = trace.GenerateToken()
	return nil
}

// Result is a non-blocking RPC from the worker that sends the solution to some previous pow instance assignment
// back to the Coordinator
func (c *CoordRPCHandler) Result(args CoordResultArgs, reply *Reply) error {
	trace := c.tracer.ReceiveToken(args.Token)

	taskKey := generateCoordTaskKey(args.Nonce, args.NumTrailingZeros)

	c.tasks.mu.Lock()
	curTask := c.tasks.tasks[taskKey]
	c.tasks.mu.Unlock()

	if args.Secret != nil {
		// For result, need an additional cancel
		curTask.Wg.Add(1)

		// Double for Coord
		trace.RecordAction(CoordinatorWorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           args.Secret,
		})

		var calleeReply *Reply
		for _, w := range c.workers {
			trace.RecordAction(CoordinatorWorkerCancel{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				WorkerByte:       w.workerByte,
			})
			args := WorkerCancelArgs{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				WorkerByte:       w.workerByte,
				Secret:           args.Secret,
				Token:            trace.GenerateToken(),
			}
			err := w.client.Call("WorkerRPCHandler.Found", args, &calleeReply)
			if err != nil {
				return err
			}
			c.tracer.ReceiveToken(calleeReply.Token)
		}
	} else {
		log.Printf("Received worker cancel ack: %v", args)
	}

	// update cache
	c.cache.update(args.Nonce, args.NumTrailingZeros, args.Secret, trace)

	curTask.Wg.Done()

	reply.Token = trace.GenerateToken()
	return nil
}

func (c *Coordinator) InitializeRPCs() error {
	handler := &CoordRPCHandler{
		tracer:     c.tracer,
		workers:    c.workers,
		workerBits: uint(math.Log2(float64(len(c.workers)))),
		tasks: TasksCache{
			tasks: make(map[string]*MineTask),
		},
		cache: Cache{
			nonceCache: make(map[string]NonceCache),
		},
	}
	server := rpc.NewServer()
	err := server.Register(handler) // publish Coordinator<->worker procs
	if err != nil {
		return fmt.Errorf("format of Coordinator RPCs aren't correct: %s", err)
	}

	workerListener, e := net.Listen("tcp", c.config.WorkerAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.WorkerAPIListenAddr, e)
	}

	clientListener, e := net.Listen("tcp", c.config.ClientAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.ClientAPIListenAddr, e)
	}

	go server.Accept(workerListener)
	server.Accept(clientListener)

	return nil
}

func initializeWorkers(workers []*WorkerClient) error {
	for _, w := range workers {
		if w.client == nil {
			client, err := rpc.Dial("tcp", string(w.addr))
			if err != nil {
				log.Printf("Waiting for worker %d", w.workerByte)
				return fmt.Errorf("failed to dial worker: %s", err)
			}
			w.client = client
		}
	}
	return nil
}

// TODO: unit test
func (c *Cache) getSecretIfExists(nonce []uint8, numTrailingZeros uint, trace *tracing.Trace) (Secret, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.nonceCache[generateCoordNonceKey(nonce)]; ok {
		if numTrailingZeros <= entry.maxNumTrailingZeros {
			trace.RecordAction(CacheHit{Nonce: nonce, NumTrailingZeros: numTrailingZeros, Secret: entry.secret})
			return entry.secret, true
		}
	}
	trace.RecordAction(CacheMiss{Nonce: nonce, NumTrailingZeros: numTrailingZeros})
	return nil, false
}

// TODO: unit test
func (c *Cache) update(nonce []uint8, numTrailingZeros uint, newSecret Secret, trace *tracing.Trace) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := generateCoordNonceKey(nonce)
	if entry, exists := c.nonceCache[key]; exists {
		if numTrailingZeros > entry.maxNumTrailingZeros {
			trace.RecordAction(CacheMiss{Nonce: nonce, NumTrailingZeros: numTrailingZeros})
			c.remove(key, nonce, entry.maxNumTrailingZeros, entry.secret, trace)
			c.add(key, nonce, numTrailingZeros, newSecret, trace)
			return
		}
		trace.RecordAction(CacheHit{Nonce: nonce, NumTrailingZeros: numTrailingZeros, Secret: entry.secret})
		if numTrailingZeros == entry.maxNumTrailingZeros {
			if dominates(newSecret, entry.secret) {
				c.remove(key, nonce, entry.maxNumTrailingZeros, entry.secret, trace)
				c.add(key, nonce, numTrailingZeros, newSecret, trace)
				return
			}
		}
		// else numTrailingZeros < entry.maxNumTrailingZeros - keep existing entry
	} else {
		trace.RecordAction(CacheMiss{Nonce: nonce, NumTrailingZeros: numTrailingZeros})
		c.add(key, nonce, numTrailingZeros, newSecret, trace)
	}
}

// assumes locks have been acquired
func (c *Cache) add(key string, nonce []uint8, numTrailingZeros uint, newSecret Secret, trace *tracing.Trace) {
	c.nonceCache[key] = NonceCache{numTrailingZeros, newSecret}
	trace.RecordAction(CacheAdd{Nonce: nonce, NumTrailingZeros: numTrailingZeros, Secret: newSecret})
}

// assumes locks have been acquired and entry with key exists
func (c *Cache) remove(key string, nonce []uint8, numTrailingZeros uint, oldSecret Secret, trace *tracing.Trace) {
	delete(c.nonceCache, key)
	trace.RecordAction(CacheRemove{Nonce: nonce, NumTrailingZeros: numTrailingZeros, Secret: oldSecret})
}

func (t *TasksCache) createTask(taskKey string) *MineTask {
	t.mu.Lock()
	defer t.mu.Unlock()

	task := &MineTask{
		Secret:        nil, // indicates a miss on subsequent requests before a result is found
		Mu:            &sync.Mutex{},
		Wg:            &sync.WaitGroup{},
		IsFirstResult: false,
	}
	t.tasks[taskKey] = task
	log.Printf("New task added: %v\n", t.tasks)
	return task
}

func (t *TasksCache) deleteTask(taskKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, taskKey)
	log.Printf("Task deleted: %v\n", t.tasks)
}

func (mt *MineTask) lock() {
	mt.Mu.Lock()
}

func (mt *MineTask) unlock() {
	mt.Mu.Unlock()
}

func generateCoordTaskKey(nonce []uint8, numTrailingZeros uint) string {
	return fmt.Sprintf("%s|%d", hex.EncodeToString(nonce), numTrailingZeros)
}

func generateCoordNonceKey(nonce []uint8) string {
	return fmt.Sprintf("%s", hex.EncodeToString(nonce))
}

// Return true if secret1 >= secret2
func dominates(secret1 Secret, secret2 Secret) bool {
	return bytes.Compare(secret1, secret2) >= 0
}
