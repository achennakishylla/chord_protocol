// Authors: Achennaki Shylla, Shintu Joseph Thattakunnil

package main

import (
	"fmt"
	"sync"
)

var wg sync.WaitGroup

var coordinateChan chan string

func main() {

	// fmt.Println("Enter the number of nodes to spawn:: ")
	// fmt.Scan(&N)
	// if N == 0 || N < 0 {
	// 	fmt.Println("Please enter positive number of nodes")
	// 	os.Exit(0)
	// }

	//coordinateChan = make(chan int)
	coordinateChan = make(chan string)

	fmt.Println("Running Coordinator")

	generateRandomID(20)

	wg.Add(1)

	go coordinator()

	injectRequests()

	//scanner := bufio.NewScanner(os.Stdin)
	//fmt.Println("Enter the order of the node size on powers of 2:")
	//scanner.Scan()
	wg.Wait()

}
