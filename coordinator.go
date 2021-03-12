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
	Wg           sync.WaitGroup
	Mu           sync.Mutex
	CachedSecret Secret
	ResultChan   ResultChan
}

type CoordRPCHandler struct {
	tracer     *tracing.Tracer
	workers    []*WorkerClient
	workerBits uint
	mineTasks  TasksCache
}

type TasksCache struct {
	mu    sync.Mutex
	tasks map[string]MineTask
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

	// initialize and connect to workers (if not already connected)
	for err := initializeWorkers(c.workers); err != nil; {
		log.Println(err)
		err = initializeWorkers(c.workers)
	}

	workerCount := len(c.workers)

	foundChan := make(chan CoordResultArgs, workerCount)
	secret, ok := c.mineTasks.create(args.Nonce, args.NumTrailingZeros, foundChan)
	if ok {
		return c.mineComplete(
			reply,
			CoordinatorSuccess{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				Secret:           secret,
			},
			trace)
	}

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

	// wait for all all workers to send back cancel ACK, ignoring results (receiving them is logged, but they have no further use here)
	// we asked all workers to cancel, so we should setResult exactly workerCount ACKs.
	workerAcksReceived := 0
	for workerAcksReceived < workerCount {
		ack := <-foundChan
		if ack.Secret == nil {
			log.Printf("Counting toward acks: %v", ack)

		} else {
			log.Printf("Received updated secret: %v", ack)
			secret = ack.Secret
		}
	}

	// delete completed mine task from map
	c.mineTasks.delete(args.Nonce, args.NumTrailingZeros)

	return c.mineComplete(
		reply,
		CoordinatorSuccess{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			Secret:           secret,
		},
		trace)
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
	if args.Secret != nil {
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
	c.mineTasks.setResult(args.Nonce, args.NumTrailingZeros, args)
	reply.Token = trace.GenerateToken()
	return nil
}

func (c *Coordinator) InitializeRPCs() error {
	handler := &CoordRPCHandler{
		tracer:     c.tracer,
		workers:    c.workers,
		workerBits: uint(math.Log2(float64(len(c.workers)))),
		mineTasks: TasksCache{
			tasks: make(map[string]MineTask),
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

func (t *TasksCache) setResult(nonce []uint8, numTrailingZeros uint, result CoordResultArgs) {
	//value, ok := t.tasks.Load(generateCoordTaskKey(nonce, numTrailingZeros))
	//if !ok {
	//	log.Fatal("No value for existing task")
	//}
	//if dominates(result.Secret, )
	//task := value.(MineTask)
	//task.ResultChan <- result
	t.mu.Lock()
	defer t.mu.Unlock()
	key := generateCoordTaskKey(nonce, numTrailingZeros)
	if taskEntry, ok := t.tasks[key]; ok {
		if taskEntry.CachedSecret == nil {
			// cache miss
		} else {
			// cache hit
		}
		taskEntry.ResultChan <- result
	} else {
		// fatal, must have an entry
	}

}

func (t *TasksCache) create(nonce []uint8, numTrailingZeros uint, val ResultChan) (Secret, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := generateCoordTaskKey(nonce, numTrailingZeros)
	if taskEntry, ok := t.tasks[key]; ok {
		secret, ok := taskEntry.checkSecret(nonce, numTrailingZeros)
		if ok {
			return secret, ok
		}
	} else {
		// TODO: cache miss
		t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)] = MineTask{
			ResultChan:   val,
			CachedSecret: nil, // indicates a miss on subsequent requests before a result is found
		}
		log.Printf("New task added: %v\n", t.tasks)
	}
	return nil, false
}

func (t *TasksCache) delete(nonce []uint8, numTrailingZeros uint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, generateCoordTaskKey(nonce, numTrailingZeros))
	log.Printf("Task deleted: %v\n", t.tasks)
}

func (mt *MineTask) checkSecret(nonce []uint8, numTrailingZeros uint) (Secret, bool) {
	mt.Mu.Lock()
	defer mt.Mu.Unlock()
	if mt.CachedSecret == nil {
		// TODO: cache miss
		return nil, false
	}
	// TODO: cache hit
	return mt.CachedSecret, true
}

func generateCoordTaskKey(nonce []uint8, numTrailingZeros uint) string {
	return fmt.Sprintf("%s|%d", hex.EncodeToString(nonce), numTrailingZeros)
}

// Return true if secret1 >= secret2
func dominates(secret1 []uint8, secret2 []uint8) bool {
	return bytes.Compare(secret1, secret2) >= 0
}
