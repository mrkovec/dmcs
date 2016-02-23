package dmcs

import (
	// "log"
	// "sort"
	// "fmt"
	// "errors"
	// "bytes"
	"encoding/binary"
	// "github.com/mrkovec/encdec"
)

type integerColumn struct {
	c *bytesliceColumn
	tent [][]byte
}

func newIntegerColumn(id int, storage []byte, offset int) *integerColumn {
	return &integerColumn{c:newBytesliceColumn(id, compareInt, storage, offset)/*, 	enc:encdec.NewEnc()*/}
}


func (i *integerColumn) filter(op relOp, n interface{}) ([]int, error) {
	var b []byte
	if op!= ALL {
		ndl, ok := n.(int64)
		if !ok {
			return nil, errWrongDataType
		}
		b = enc(ndl)
	}
	return i.c.filter(op, b)
}

func (i *integerColumn) delete(tuplePos []int) error {
	return i.c.delete(tuplePos)
}

func (i *integerColumn) read(tuplePos []int, r interface{}) (interface{}, error) {
	e, err := i.c.read(tuplePos, r)
	if err != nil {
		return nil, err
	}
	ent := e.([][]byte)
	ie := make([]int64, len(ent))
	for j := 0; j < len(ent); j++ {
		ie[j] = dec(ent[j])
	}
	return ie, nil
}

func (i *integerColumn) inspect(e interface{}, tuplePos []int, op relOp, ndl interface{}) error {
	if e != nil {
		ent := e.([]int64)
		if cap(i.tent) < len(ent) {
			i.tent = make([][]byte, len(ent))	
		}
		// var buf [binary.MaxVarintLen64]byte
	 
		i.tent = i.tent[:len(ent)]
		for j := 0; j < len(ent); j++ {
			i.tent[j] = enc(ent[j])
			// i.tent[j] = buf[:binary.PutVarint(buf[:], ent[j])]
			// i.tent = append(i.tent, [][]byte{enc(ent[j])})
		}	
		return i.c.inspect(i.tent, tuplePos, op, ndl)
	}
	return i.c.inspect(e, tuplePos, op, ndl)
}

func (i *integerColumn) create(e interface{}) error {
	_, ok := e.([]int64)
	if !ok {
		return errWrongDataType
	}	

	if err := i.inspect(e, nil, 0, nil); err != nil {
		return err
	}
	// be := make([][]byte, len(ent))
	// for j := 0; j < len(ent); j++ {
	// 	be[j] = enc(ent[j])
	// }
	return i.c.create(i.tent)
}

func (i *integerColumn) len() (int, int) {
	return i.c.len()
}

func (i *integerColumn) String() string {
	return i.c.String()
}

func (i *integerColumn) size() int {
	return i.c.size()
}

func enc(n int64) []byte {
	var buf [binary.MaxVarintLen64]byte
	return buf[:binary.PutVarint(buf[:], n)]
}
func dec(b []byte) (r int64) {
	r, _ = binary.Varint(b)
	return r
}
func compareInt(a, b []byte) int {
	// da, db := encdec.NewDec(a), encdec.NewDec(b)
	ia, ib := dec(a), dec(b)//da.Int64(), db.Int64()
	if ia == ib {
		return 0
	}
	if ia > ib {
		return 1
	}
	return -1
}