// Authors: Achennaki Shylla, Shintu Joseph Thattakunnil

package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

var ticker *time.Ticker

type doMsgs struct {
	Do string
}

type joinRingMsg struct {
	Do      string
	Sponsor HashKey
}

type leaveRingMsg struct {
	Do        string
	Mode      string
	Recipient HashKey
}

type doRespondToMsgs struct {
	Do        string
	RespondTO HashKey
}

type findRingSPMsg struct {
	Do        string
	RespondTO HashKey
	TargetID  HashKey
}

type putMsg struct {
	Do        string
	RespondTO HashKey
	Data      dataMsg2
}

type getRemMsgs struct {
	Do        string
	RespondTO HashKey
	Data      dataMsg1
}

type dataMsg1 struct {
	Key HashKey
}

type dataMsg2 struct {
	Key   HashKey
	Value string
}

type updateBucketsAndPredecessorMsg struct {
	Do          string
	BucketData  map[HashKey]string
	Predecessor HashKey
}

type updatePredecessorMsg struct {
	Do          string
	Predecessor HashKey
}

type updateSuccessorMsg struct {
	Do        string
	Successor HashKey
}

type getBucketMessage struct {
	Do  string
	Key HashKey
}

type copyBucketMessage struct {
	Do     string
	Bucket map[HashKey]string
}

func injectRequests() {
	ticker = time.NewTicker(1500 * time.Millisecond)
	c := 0
	go func() {
		for t := range ticker.C {
			//coordinateChan <- generateMessages(t)
			if c == 0 {
				coordinateChan <- generateRandomMessage(t)
				c++
			}
		}
	}()
}

func generateMessages(timeSeed time.Time) int {
	return randomGenerator(timeSeed, 0, 21)
}

//Generate messages
func generateRandomMessage(timeSeed time.Time) string {

	var message string
	min := 0
	max := 5
	rand.Seed(timeSeed.UTC().UnixNano())
	choice := rand.Intn(max-min) + min

	switch choice {
	case 0:
		{
			sponsorKey := nodeList[rand.Intn(len(nodeList))]

			msg0 := &joinRingMsg{
				Do:      "join-ring",
				Sponsor: sponsorKey,
			}
			marshalledMessage, _ := json.Marshal(msg0)
			message = string(marshalledMessage)
			fmt.Println("Case join-ring")
			//return joinRingMessage
		}
	case 1:
		{
			fmt.Println("Case leave-ring")
			ch := rand.Intn(2)
			switch ch {
			case 0:
				{
					mode := "immediate"
					msg1 := &leaveRingMsg{
						Do:   "leave-ring",
						Mode: mode,
					}
					marshalledMessage, _ := json.Marshal(msg1)

					message = string(marshalledMessage)
					//return leaveRingMessage
				}
			case 1:
				{
					mode := "orderly"
					msg1 := &leaveRingMsg{
						Do:        "leave-ring",
						Recipient: 4022502477,
						Mode:      mode,
					}
					marshalledMessage, _ := json.Marshal(msg1)
					message = string(marshalledMessage)

				}
			}
		}
	case 2:
		{
			sponsor := nodeList[rand.Intn(len(nodeList))]
			key := nodeList[rand.Intn(len(nodeList))]
			datMsg := dataMsg2{
				Key:   key,
				Value: "val",
			}
			msg9 := &putMsg{
				Do:        "put",
				RespondTO: sponsor,
				Data:      datMsg,
			}
			marshalledMessage, _ := json.Marshal(msg9)
			message = string(marshalledMessage)
			fmt.Println("Case put data")
		}
	case 3:
		{
			sponsor := nodeList[rand.Intn(len(nodeList))]
			key := nodeList[rand.Intn(len(nodeList))]
			datMsg := dataMsg1{
				Key: key,
			}
			msg10 := &getRemMsgs{
				Do:        "get",
				RespondTO: sponsor,
				Data:      datMsg,
			}
			marshalledMessage, _ := json.Marshal(msg10)
			message = string(marshalledMessage)
			fmt.Println("Case get data")
		}
	case 4:
		{
			sponsor := nodeList[rand.Intn(len(nodeList))]
			key := nodeList[rand.Intn(len(nodeList))]
			datMsg := dataMsg1{
				Key: key,
			}
			msg11 := &getRemMsgs{
				Do:        "remove",
				RespondTO: sponsor,
				Data:      datMsg,
			}
			marshalledMessage, _ := json.Marshal(msg11)
			message = string(marshalledMessage)
			fmt.Println("Case remove data")
		}
	}
	fmt.Println("Message: ", message)
	return message
}

func randomGenerator(timeSeed time.Time, min int, max int) int {
	rand.Seed(timeSeed.UTC().UnixNano())
	return rand.Intn(max-min) + min
}

func triggerSuccesorMessage(sponsor HashKey, recipient HashKey) string {
	findSuccesorM := &findRingSPMsg{
		Do:        "find-ring-successor",
		RespondTO: sponsor,
		TargetID:  recipient,
	}
	fsMessage, _ := json.Marshal(findSuccesorM)
	return string(fsMessage)
}

func initRingFingMessage() string {
	msg := &doMsgs{
		Do: "init-ring-fingers",
	}
	initRingMessage, _ := json.Marshal(msg)
	return string(initRingMessage)
}

func getRingFingMessage(key HashKey) string {

	msg := &doRespondToMsgs{
		Do:        "get-ring-fingers",
		RespondTO: key,
	}
	getRingFinMessage, _ := json.Marshal(msg)
	return string(getRingFinMessage)

}

func updateBucketAndPredecessorMessage(bucketData map[HashKey]string, predecessor HashKey) string {

	msg := &updateBucketsAndPredecessorMsg{
		Do:          "update-bucket-and-predecessor",
		BucketData:  bucketData,
		Predecessor: predecessor,
	}
	updateBucketsAndPredecessorMsg, _ := json.Marshal(msg)
	return string(updateBucketsAndPredecessorMsg)

}

func triggerPredecessorMessage(sponsor HashKey, recipient HashKey) string {
	findPredecessorM := &findRingSPMsg{
		Do:        "find-ring-predecessor",
		RespondTO: sponsor,
		TargetID:  recipient,
	}
	fsMessage, _ := json.Marshal(findPredecessorM)
	return string(fsMessage)
}

func updateSuccessorMessage(successor HashKey) string {

	msg := &updateSuccessorMsg{
		Do:        "update-successor",
		Successor: successor,
	}
	updateSuccessorMsg, _ := json.Marshal(msg)
	return string(updateSuccessorMsg)

}

func updatePredecessorMessage(predecessor HashKey) string {

	msg := &updatePredecessorMsg{
		Do:          "update-predecessor",
		Predecessor: predecessor,
	}
	updatePredecessorMsg, _ := json.Marshal(msg)
	return string(updatePredecessorMsg)

}

func triggerGetBucktMessage(recipient HashKey) string {
	getBuckt := &getBucketMessage{
		Do:  "get-bucket",
		Key: recipient,
	}
	gbMessage, _ := json.Marshal(getBuckt)
	return string(gbMessage)
}

func triggerCopyBucktMessage(successor HashKey, recipient HashKey, bucket map[HashKey]string) string {
	buck := make(map[HashKey]string)
	for b, v := range bucket {
		if b >= recipient && b < successor {
			buck[b] = v
		}
	}
	copyBuckt := &copyBucketMessage{
		Do:     "copy-bucket",
		Bucket: buck,
	}
	cbMessage, _ := json.Marshal(copyBuckt)
	return string(cbMessage)
}
