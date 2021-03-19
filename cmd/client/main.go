package main

import (
	"flag"
	"log"

	"example.org/cpsc416/a2/powlib"

	distpow "example.org/cpsc416/a2"
)

func setupClient(id string, clientConfig string) *distpow.Client {
	var config distpow.ClientConfig
	err := distpow.ReadJSONConfig(clientConfig, &config)
	if err != nil {
		log.Fatal(err)
	}
	client := distpow.NewClient(config, powlib.NewPOW())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	return client
}

func main() {

	var c1 = flag.Bool("c1", false, "flag indicating which client to choose")

	var c2 = flag.Bool("c2", false, "flag indicating which client to choose")

	var c3 = flag.Bool("c3", false, "flag indicating which client to choose")

	var c4 = flag.Bool("c4", false, "flag indicating which client to choose")

	flag.Parse()

	var client *distpow.Client
	var client2 *distpow.Client
	var client3 *distpow.Client
	var client4 *distpow.Client

	client = setupClient("id", "config/client_config.json")
	defer client.Close()

	client2 = setupClient("id2", "config/client2_config.json")

	client3 = setupClient("id3", "config/client3_config.json")
	defer client3.Close()

	client4 = setupClient("id4", "config/client4_config.json")
	defer client4.Close()

	endWaitCount := 0

	if *c1 {
		endWaitCount += 2
		if err := client.Mine([]uint8{1, 2, 3, 4}, 4); err != nil {
			log.Println(err)
		}
		if err := client.Mine([]uint8{5, 6, 7, 8}, 5); err != nil {
			log.Println(err)
		}
	}

	if *c4 {
		endWaitCount += 2
		if err := client4.Mine([]uint8{5, 3, 2, 31}, 1); err != nil {
			log.Println(err)
		}

		if err := client4.Mine([]uint8{2, 4, 4, 1}, 3); err != nil {
			log.Println(err)
		}
	}

	if *c3 {
		endWaitCount += 1
		if err := client3.Mine([]uint8{3, 3, 3, 3}, 5); err != nil {
			log.Println(err)
		}
		<-client3.NotifyChannel
		if err := client3.Mine([]uint8{3, 3, 3, 3}, 5); err != nil {
			log.Println(err)
		}
	}

	if *c2 {
		endWaitCount += 1
		if err := client2.Mine([]uint8{2, 2, 2, 2}, 6); err != nil {
			log.Println(err)
		}
		<-client2.NotifyChannel
		if err := client2.Mine([]uint8{2, 2, 2, 2}, 5); err != nil {
			log.Println(err)
		}
		<-client2.NotifyChannel
		if err := client2.Mine([]uint8{2, 2, 2, 2}, 7); err != nil {
			log.Println(err)
		}
	}

	for i := 0; i < endWaitCount; i++ {
		select {
		case mineResult := <-client.NotifyChannel:
			log.Println(mineResult)
		case mineResult := <-client2.NotifyChannel:
			log.Println(mineResult)
		case mineResult := <-client3.NotifyChannel:
			log.Println(mineResult)
		case mineResult := <-client4.NotifyChannel:
			log.Println(mineResult)
		}
	}
}
