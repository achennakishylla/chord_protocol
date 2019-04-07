// Authors: Achennaki Shylla, Shintu Joseph Thattakunnil

package main

import (
	"math"
)

func copyFingerTable(node HashKey, successor HashKey, fingerTable []HashKey, successorFingerTable []HashKey) {

	for i, v := range successorFingerTable {

		calculatedFinger := HashKey((int(node) + int(math.Pow(2, float64(i)))) % int(math.Pow(2, 32)))

		if calculatedFinger <= successor {
			fingerTable[i] = successor
		} else {
			fingerTable[i] = v
		}

	}
}

func removeNodeFromList(node HashKey) {
	index := -1
	for i, v := range nodeList {
		if v == node {
			index = i
		}
	}
	nodeList = append(nodeList[:index], nodeList[index+1:]...)
}
