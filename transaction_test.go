package dmcs

import (
	"testing"
 	"log"
)

func TestTransactionBasicOp(t *testing.T) {
	emp := NewColumnFamily(map[ColumnId]ColumnType{empEmpno:ByteSlice})
	body := func(trx *transaction) error {
		return trx.Create([]ColumnId{empEmpno}, []interface{}{[][]byte{[]byte{'a'}, []byte{'b'}, []byte{'d'}}})
	}
	trx, err := newTransaction(body, emp)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(trx)
}
 