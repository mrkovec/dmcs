package dmcs

import (
	"testing"
	"fmt"
	"time"
	// "log"
 )

func TestTimeColumnBasicOp(t *testing.T) {
	c := newTimeColumn(0, make([]byte,  64), 0)
 
 	t1, _ := time.Parse("2006-Jan-02", "2016-Feb-01")
 	t2, _ := time.Parse("2006-Jan-02", "2016-Feb-02")
 	t3, _ := time.Parse("2006-Jan-02", "2016-Feb-03")
 	t4, _ := time.Parse("2006-Jan-02", "2016-Feb-10")
	
	err := c.create([]time.Time{t2, t3, t1, t2})
	if err != nil {
		t.Fatal(err)
	} 
	dt, err := c.read([]int{2, 0, 3, 1}, nil)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = fmt.Sprintf("[%v %v %v %v]", t1, t2, t2, t3), fmt.Sprintf("%v", dt)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}

 	tuplepos, err := c.filter(EQUAL, t1)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[2]", fmt.Sprintf("%v", tuplepos)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 	tuplepos, err = c.filter(NOTEQUAL, t1)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
	tuplepos, err = c.filter(LESS, t4)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 2 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 	
	tuplepos, err = c.filter(LESSEQUAL, t2)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 2 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 	 	
	tuplepos, err = c.filter(GREATER, t1)
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[0 1 3]", fmt.Sprintf("%v", sortTP(tuplepos))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	} 
	tuplepos, err = c.filter(GREATEREQUAL, t2)
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
	e, g = fmt.Sprintf("[%v %v %v]", t1, t2, t2), fmt.Sprintf("%v", dt)
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
 
}
