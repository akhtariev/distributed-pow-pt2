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

	if *c1 {
		client = setupClient("id", "config/client_config.json")
		defer client.Close()
	}

	if *c2 {
		client2 = setupClient("id2", "config/client2_config.json")
		defer client2.Close()
	}

	if *c3 {
		client3 = setupClient("id3", "config/client3_config.json")
		defer client3.Close()
	}

	if *c4 {
		client4 = setupClient("id4", "config/client4_config.json")
		defer client4.Close()
	}

	c1Wait := 0
	c2Wait := 0
	c3Wait := 0
	c4Wait := 0

	if *c1 {
		c1Wait += 2
		if err := client.Mine([]uint8{1, 2, 3, 4}, 4); err != nil {
			log.Println(err)
		}
		if err := client.Mine([]uint8{5, 6, 7, 8}, 5); err != nil {
			log.Println(err)
		}
	}

	if *c4 {
		c4Wait += 2
		if err := client4.Mine([]uint8{5, 3, 2, 31}, 1); err != nil {
			log.Println(err)
		}

		if err := client4.Mine([]uint8{2, 4, 4, 1}, 3); err != nil {
			log.Println(err)
		}
	}

	if *c3 {
		c3Wait += 1
		if err := client3.Mine([]uint8{3, 3, 3, 3}, 5); err != nil {
			log.Println(err)
		}
		<-client3.NotifyChannel
		if err := client3.Mine([]uint8{3, 3, 3, 3}, 5); err != nil {
			log.Println(err)
		}
	}

	if *c2 {
		c2Wait += 1
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

	for i := 0; i < c1Wait; i++ {
		<-client.NotifyChannel
	}

	for i := 0; i < c2Wait; i++ {
		<-client2.NotifyChannel
	}

	for i := 0; i < c3Wait; i++ {
		<-client3.NotifyChannel
	}

	for i := 0; i < c4Wait; i++ {
		<-client4.NotifyChannel
	}
}
