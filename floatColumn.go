package dmcs

import (
	// "log"
	// "sort"
	// "fmt"
	"errors"
	// "bytes"
	"encoding/binary"
	"math"
	// "github.com/mrkovec/encdec"
)



type floatColumn struct {
	c *bytesliceColumn
}

func encfloat(n float64) []byte {
	var buf [binary.MaxVarintLen64]byte
	return buf[:binary.PutUvarint(buf[:], math.Float64bits(n))]
}
func decfloat(b []byte) (r float64) {
	n, _ := binary.Uvarint(b)
	r = math.Float64frombits(n)
	return r
}
func compareFloat(a, b []byte) int {
	ia, ib := decfloat(a), decfloat(b)
	if ia == ib {
		return 0
	}
	if ia > ib {
		return 1
	}
	return -1
}

func newFloatColumn(id int, storage []byte, offset int) *floatColumn {
	return &floatColumn{c:newBytesliceColumn(id, compareFloat, storage, offset)/*, 	enc:encdec.NewEnc()*/}
}


func (i *floatColumn) filter(op relOp, n interface{}) ([]int, error) {
	var b []byte
	if op!= ALL {
		ndl, ok := n.(float64)
		if !ok {
			return nil, errors.New("floatColumn operates only on float64 data")
		}
		
	 // 	i.enc.Reset()
		// i.enc.Int64(ndl)
		// if i.enc.Error() != nil {
		//  	return nil, i.enc.Error()
		// }
		b = encfloat(ndl)
	}
	return i.c.filter(op, b)
}

func (i *floatColumn) delete(tuplePos []int) error {
	return i.c.delete(tuplePos)
}

func (i *floatColumn) read(tuplePos []int, r interface{}) (interface{}, error) {
	e, err := i.c.read(tuplePos, r)
	if err != nil {
		return nil, err
	}
	ent := e.([][]byte)
	ie := make([]float64, len(ent))
	for j := 0; j < len(ent); j++ {
		// i.dec = encdec.NewDec(ent[j])
		// ie[j] = i.dec.Int64()
		//  if i.dec.Error() != nil {
		//  	return nil, i.dec.Error()
		//  }
		ie[j] = decfloat(ent[j])
	}
	return ie, nil
}

func (i *floatColumn) inspect(e interface{}) error {
	ent, ok := e.([]float64)
	if !ok {
		return errors.New("floatColumn operates only on float64 data")
	}	
	if len(ent) == 0 {
		return errors.New("empty entity slice for creating")
	}
	if len(i.c.entity) + len(ent) > cap(i.c.byteData) {
		return errColDataFull
	}
	return nil
}

func (i *floatColumn) create(e interface{}) error {
	if err := i.inspect(e); err != nil {
		return err
	}
	ent := e.([]float64)
	be := make([][]byte, len(ent))
	for j := 0; j < len(ent); j++ {
		 // i.enc.Reset()
		 // i.enc.Int64(ent[j])
		 // if i.enc.Error() != nil {
		 // 	return i.enc.Error()
		 // }
		 // be[j] = i.enc.Bytes()
		be[j] = encfloat(ent[j])
	}
	return i.c.create(be)
}

func (i *floatColumn) len() int {
	return i.c.len()
}

func (i *floatColumn) String() string {
	return i.c.String()
}


 