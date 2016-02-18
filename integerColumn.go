package dmcs

import (
	// "log"
	// "sort"
	// "fmt"
	"errors"
	// "bytes"
	"encoding/binary"
	// "github.com/mrkovec/encdec"
)



type integerColumn struct {
	c *bytesliceColumn
	// enc *encdec.Enc
	// dec *encdec.Dec
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

func newIntegerColumn(id int, storage []byte, offset int) *integerColumn {
	return &integerColumn{c:newBytesliceColumn(id, compareInt, storage, offset)/*, 	enc:encdec.NewEnc()*/}
}


func (i *integerColumn) filter(op relOp, n interface{}) ([]int, error) {
	var b []byte
	if op!= ALL {
		ndl, ok := n.(int64)
		if !ok {
			return nil, errors.New("integerColumn operates only on int64 data")
		}
		
	 // 	i.enc.Reset()
		// i.enc.Int64(ndl)
		// if i.enc.Error() != nil {
		//  	return nil, i.enc.Error()
		// }
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
		// i.dec = encdec.NewDec(ent[j])
		// ie[j] = i.dec.Int64()
		//  if i.dec.Error() != nil {
		//  	return nil, i.dec.Error()
		//  }
		ie[j] = dec(ent[j])
	}
	return ie, nil
}

func (i *integerColumn) inspect(e interface{}) error {
	ent, ok := e.([]int64)
	if !ok {
		return errors.New("integerColumn operates only on int64 data")
	}	
	if len(ent) == 0 {
		return errors.New("empty entity slice for creating")
	}
	if len(i.c.entity) + len(ent) > cap(i.c.byteData) {
		return errColDataFull
	}
	return nil
}

func (i *integerColumn) create(e interface{}) error {
	if err := i.inspect(e); err != nil {
		return err
	}
	ent := e.([]int64)
	be := make([][]byte, len(ent))
	for j := 0; j < len(ent); j++ {
		 // i.enc.Reset()
		 // i.enc.Int64(ent[j])
		 // if i.enc.Error() != nil {
		 // 	return i.enc.Error()
		 // }
		 // be[j] = i.enc.Bytes()
		be[j] = enc(ent[j])
	}
	return i.c.create(be)
}

func (i *integerColumn) len() int {
	return i.c.len()
}

func (i *integerColumn) String() string {
	return i.c.String()
}


 