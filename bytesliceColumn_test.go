package dmcs

import (
	"testing"
	"fmt"
)


func TestBytesliceColumnBasicOp(t *testing.T) {
	c := newBytesliceColumn(0, compareBytes, make([]byte, 0, 1024), 0)

	err := c.create([][]byte{[]byte{0, 1, 2},[]byte{6, 7, 8}, []byte{3, 4, 5}})
	if err != nil {
		t.Fatal(err)
	} 
	// log.Println(c)
 	
 	dt, err := c.read([]int{0, 2, 1}, nil)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[[0 1 2] [3 4 5] [6 7 8]]", fmt.Sprintf("%v", dt)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
	
	tuplepos, err := c.filter(EQUAL, []byte{6, 7, 8})
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[1]", fmt.Sprintf("%v", tuplepos)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 	tuplepos, err = c.filter(NOTEQUAL, []byte{6, 7, 8})
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 2]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 	tuplepos, err = c.filter(EQUAL, []byte{9, 10, 11})
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[]", fmt.Sprintf("%v", tuplepos)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 	tuplepos, err = c.filter(NOTEQUAL, []byte{9, 10, 11})
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 2]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
	
	err = c.delete([]int{1})
	if err != nil {
		t.Fatal(err)
	} 
	tuplepos, _ = c.filter(ALL, nil)
	dt, _ = c.read(tuplepos, nil)
	e, g = "[[0 1 2] [3 4 5]]", fmt.Sprintf("%v", dt)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
}
 