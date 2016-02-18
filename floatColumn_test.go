package dmcs

import (
	"testing"
	"fmt"
	// "log"
 )

func TestFloatColumnBasicOp(t *testing.T) {
	c := newFloatColumn(0, make([]byte,  64), 0)

	err := c.create([]float64{1, 2, -3, 1})
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

 	tuplepos, err := c.filter(EQUAL, float64(-3))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[2]", fmt.Sprintf("%v", tuplepos)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 	tuplepos, err = c.filter(NOTEQUAL, float64(-3))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
	tuplepos, err = c.filter(LESS, float64(3))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 2 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 	
	tuplepos, err = c.filter(LESSEQUAL, float64(1))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 2 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 	 	
	tuplepos, err = c.filter(GREATER, float64(0))
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 
	tuplepos, err = c.filter(GREATEREQUAL, float64(1))
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
