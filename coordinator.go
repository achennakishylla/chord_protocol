// Authors: Achennaki Shylla, Shintu Joseph Thattakunnil

package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
)

//var channelMap map[HashKey]chan string
var channelMap map[HashKey]chan string

func initGlobals() {
	//channelMap = make(map[HashKey]chan string)
	channelMap = make(map[HashKey]chan string)
}

func coordinator() {
	defer wg.Done()
	initGlobals()

	for i := 0; i < len(nodeList); i++ {
		key := nodeList[i]
		channelMap[key] = make(chan string, 5)
	}
	for i := 0; i < len(nodeList); i++ {
		key := nodeList[i]
		wg.Add(1)
		go nodeWorker(key, true)
	}

	//Send message to sponsor
	for message := range coordinateChan {
		var dat map[string]interface{}
		if err := json.Unmarshal([]byte(message), &dat); err != nil {
			panic(err)
		}
		if dat["Do"] == "join-ring" {

			key := genKey(randString())
			channelMap[key] = make(chan string, 5)
			wg.Add(1)
			go nodeWorker(key, false)
			channelMap[key] <- message
			channelMap[key] <- initRingFingMessage()
		}
		if dat["Do"] == "leave-ring" {
			channelMap[4022502477] <- message
		}
		if dat["Do"] == "put" {
			sponsor := nodeList[rand.Intn(len(nodeList))]
			channelMap[sponsor] <- message
		}
		if dat["Do"] == "get" {
			sponsor := nodeList[rand.Intn(len(nodeList))]
			channelMap[sponsor] <- message
		}
		if dat["Do"] == "remove" {
			sponsor := nodeList[rand.Intn(len(nodeList))]
			channelMap[sponsor] <- message
		}

	}
}

func closeAllChannels() {
	fmt.Println("s")
	ticker.Stop()
	for _, channel := range channelMap {
		close(channel)
	}
	close(coordinateChan)
}
