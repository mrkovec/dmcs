package dmcs

import (
	"testing"
	"fmt"
	// "log"
 )

func TestIntegerColumnBasicOp(t *testing.T) {
	c := newIntegerColumn(0, make([]byte,0, 64), 0)

	err := c.create([]int64{1, 2, -3, 1})
	if err != nil {
		t.Fatal(err)
	} 
	dt, err := c.read([]int{2, 0, 3, 1}, nil)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[-3 1 1 2]", fmt.Sprintf("%v", dt)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}

 	tuplepos, err := c.filter(EQUAL, int64(-3))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[2]", fmt.Sprintf("%v", tuplepos)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 	tuplepos, err = c.filter(NOTEQUAL, int64(-3))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
	tuplepos, err = c.filter(LESS, int64(3))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 2 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 	
	tuplepos, err = c.filter(LESSEQUAL, int64(1))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 2 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 	 	
	tuplepos, err = c.filter(GREATER, int64(0))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 
	tuplepos, err = c.filter(GREATEREQUAL, int64(1))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 	 	

 	err = c.delete([]int{1})
	if err != nil {
		t.Fatal(err)
	} 
	tuplepos, err = c.filter(ALL, nil)
	if err != nil {
		t.Fatal(err)
	} 
	dt, err = c.read(tuplepos, nil)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[-3 1 1]", fmt.Sprintf("%v", dt)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 
}
