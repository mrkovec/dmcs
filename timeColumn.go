package dmcs

import (
	// "log"
	// "sort"
	// "fmt"
	"errors"
	// "bytes"
	// "encoding/binary"
	// "math"
	// "github.com/mrkovec/encdec"
	"time"
)



type timeColumn struct {
	c *bytesliceColumn
}

func enctime(n time.Time) ([]byte, error) {
	return n.MarshalBinary()
}
func dectime(b []byte) (r time.Time, err error) {
	err = r.UnmarshalBinary(b)
	return r, err
}
func compareTime(a, b []byte) int {
	ia, _ := dectime(a)
	ib, _ := dectime(b)
	if ia.Equal(ib) {
		return 0
	}
	if ia.After(ib) {
		return 1
	}
	return -1
}

func newTimeColumn(id int, storage []byte, offset int) *timeColumn {
	return &timeColumn{c:newBytesliceColumn(id, compareTime, storage, offset)/*, 	enc:encdec.NewEnc()*/}
}


func (i *timeColumn) filter(op relOp, n interface{}) ([]int, error) {
	var ( 
		b []byte
		err error
	)
	if op!= ALL {
		ndl, ok := n.(time.Time)
		if !ok {
			return nil, errors.New("timeColumn operates only on time.Time data")
		}
		
	 // 	i.enc.Reset()
		// i.enc.Int64(ndl)
		// if i.enc.Error() != nil {
		//  	return nil, i.enc.Error()
		// }
		b, err = enctime(ndl)
		if err != nil {
			return nil, err
		}
	}
	return i.c.filter(op, b)
}

func (i *timeColumn) delete(tuplePos []int) error {
	return i.c.delete(tuplePos)
}

func (i *timeColumn) read(tuplePos []int, r interface{}) (interface{}, error) {
	e, err := i.c.read(tuplePos, r)
	if err != nil {
		return nil, err
	}
	ent := e.([][]byte)
	ie := make([]time.Time, len(ent))
	for j := 0; j < len(ent); j++ {
		// i.dec = encdec.NewDec(ent[j])
		// ie[j] = i.dec.Int64()
		//  if i.dec.Error() != nil {
		//  	return nil, i.dec.Error()
		//  }
		ie[j], err = dectime(ent[j])
		if err != nil {
			return nil, err
		}
	}
	return ie, nil
}

func (i *timeColumn) inspect(e interface{}) error {
	ent, ok := e.([]time.Time)
	if !ok {
		return errors.New("timeColumn operates only on time.Time data")
	}	
	if len(ent) == 0 {
		return errors.New("empty entity slice for creating")
	}
	if len(i.c.entity) + len(ent) > cap(i.c.byteData) {
		return errColDataFull
	}
	return nil
}

func (i *timeColumn) create(e interface{}) (err error) {
	if err := i.inspect(e); err != nil {
		return err
	}
	ent := e.([]time.Time)
	be := make([][]byte, len(ent))
	for j := 0; j < len(ent); j++ {
		 // i.enc.Reset()
		 // i.enc.Int64(ent[j])
		 // if i.enc.Error() != nil {
		 // 	return i.enc.Error()
		 // }
		 // be[j] = i.enc.Bytes()
		be[j], err = enctime(ent[j])
		if err != nil {
			return err
		}

	}
	return i.c.create(be)
}

func (i *timeColumn) len() int {
	return i.c.len()
}

func (i *timeColumn) String() string {
	return i.c.String()
}


 