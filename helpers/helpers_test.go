package helpers

import (
	"fmt"
//	"testing"
)

func ExampleMap() {
	a := []int{1, 2, 3, 4}

	//Multiply everything by 2
	b := Map(a, func(val interface{}) interface{} {
		return val.(int) * 2
	})

	fmt.Println("MAP:", b)
	//Output: MAP: [2 4 6 8]
}

func ExampleReduce() {
	//Summation
	b:=[]int{2,4,6,8}
	c := Reduce(b, 0, func(val interface{}, memo interface{}) interface{} {
		return memo.(int) + val.(int)
	})

	fmt.Println("REDUCE:", c)
	//Output: REDUCE: 20
}

func ExampleFilter() {
	//Check if the number is divisble by 4
	b:=[]int{2,4,6,8}
	d := Filter(b, func(val interface{}) bool {
		return val.(int)%4 == 0
	})

	fmt.Println("FILTER:", d)
	//Output: FILTER: [4 8]
}

