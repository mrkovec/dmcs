package dmcs

import (
	"testing"
 	"log"
)

func TestTransactionLocking(t *testing.T) {
	emp := NewColumnFamilyLogged(map[ColumnId]ColumnType{empEmpno:ByteSlice})

	// bodyExclusive := func(trx *transaction) error {
	// 	return trx.Create([]ColumnId{empEmpno}, []interface{}{[][]byte{[]byte{0}, []byte{1}, []byte{2}}})
	// }

	trx, err := newTransaction(func(trx *transaction) error {
		return trx.Create([]ColumnId{empEmpno}, []interface{}{[][]byte{[]byte{0}, []byte{1}, []byte{2}}})
	}, emp)
	if err != nil {
		t.Fatal(err)
	}
	err = trx.inspect()
	if err != nil {
		t.Fatal(err)
	}
	e, g = free, trx.state
	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}	 	
	
	// bodyExclusive = func(trx *transaction) error {
	// 	return trx.Create([]ColumnId{empEmpno}, []interface{}{[][]byte{[]byte{3}, []byte{4}, []byte{5}}})
	// }
	trx2, err := newTransaction(func(trx *transaction) error {
		return trx.Create([]ColumnId{empEmpno}, []interface{}{[][]byte{[]byte{3}, []byte{4}, []byte{5}}})
	}, emp)
	if err != nil {
		t.Fatal(err)
	}
	err = trx2.inspect()
	if err != nil {
		t.Fatal(err)
	}
	e, g = blocked, trx2.state
	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}	 	
log.Println(emp)
 	err = trx.finish(nil)
	if err != nil {
		t.Fatal(err)
	}
log.Println(emp)	
	err = trx2.inspect()
	if err != nil {
		t.Fatal(err)
	}
	e, g = free, trx2.state
	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}	 	
log.Println(emp)
	// bodyShared := func(trx *transaction) error {
	// 	_, err := trx.Read([]ColumnId{empEmpno}, [][]int{[]int{0}})
	// 	return err
	// }
	trx, err = newTransaction(func(trx *transaction) error {
		_, err := trx.Read([]ColumnId{empEmpno}, [][]int{[]int{0}})
		return err
	}, emp)
	if err != nil {
		t.Fatal(err)
	}
	err = trx.inspect()
	if err != nil {
		t.Fatal(err)
	}
	e, g = free, trx.state
	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}	 	
	
	trx2, err = newTransaction(func(trx *transaction) error {
		_, err := trx.Read([]ColumnId{empEmpno}, [][]int{[]int{0}})
		return err
	}, emp)
	if err != nil {
		t.Fatal(err)
	}
	err = trx2.inspect()
	if err != nil {
		t.Fatal(err)
	}
	e, g = free, trx2.state
	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}	 	
	
	trx3, err := newTransaction(func(trx *transaction) error {
		return trx.Delete([]int{0})
	}, emp)
	if err != nil {
		t.Fatal(err)
	}
	err = trx3.inspect()
	if err != nil {
		t.Fatal(err)
	}
	e, g = blocked, trx3.state
	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}	 	
	log.Println(emp)
}
 