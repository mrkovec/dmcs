package dmcs

import (
	"testing"
	"fmt"

)
var e, g interface{}

func TestBoolColumnBasicOp(t *testing.T) {
	c := newBoolColumn(0, make([]byte,  64), 0)

	err := c.create([]bool{true, false, true})
	if err != nil {
		t.Fatal(err)
	} 

	dt, err := c.read([]int{1, 0, 2}, nil)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[false true true]", fmt.Sprintf("%v", dt)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}

 	tuplepos, err := c.filter(EQUAL, true)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[2 0]", fmt.Sprintf("%v", tuplepos)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 	tuplepos, err = c.filter(NOTEQUAL, false)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[2 0]", fmt.Sprintf("%v", tuplepos)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}

 	err = c.delete([]int{1})
	if err != nil {
		t.Fatal(err)
	} 
	tuplepos, _ = c.filter(ALL, nil)
	dt, _ = c.read(tuplepos, nil)
	e, g = "[true true]", fmt.Sprintf("%v", dt)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 
}

/*func must(a interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return a
}*/