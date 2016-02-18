package dmcs

import (
	"sort"
)

type relOp int 

const (
	ALL relOp = iota
	EQUAL 
	NOTEQUAL 
	LESS
	LESSEQUAL
	GREATER
	GREATEREQUAL
)

func sortTP(tuplePos []int) []int {
	/*if sort.IntsAreSorted(tuplePos) {
		return tuplePos
	}*/
 	t := make([]int, len(tuplePos))
 	copy(t, tuplePos)
	sort.Ints(t)
	return t
}