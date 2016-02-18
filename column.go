package dmcs

import (
	// "log"
	"fmt"
	"errors"
)

type columnType int
const (
	BOOL columnType = iota
	BYTESLICE
	INTEGER
	FLOAT
	TIME
)

var (
	errColDataFull = errors.New("column data full")
)

type compareFunc func([]byte, []byte) int 



type columnFamily struct {
	def map[int]columnType
	block *columnBlock
}
func newColumnFamily(c map[int]columnType) *columnFamily {
	return &columnFamily{def:c, block:newColumnBlock(c, nil)}
}

func (c *columnBlock) String() string {
	s := fmt.Sprintf("\nblk %p", c)
	for _, cl := range c.columns {
		s = s + fmt.Sprintf("%v", cl)
	}
	return s
}

func (c *columnFamily) filter(cid []int, op []relOp, ndl []interface{}) ([][]int, error) {
	if len(cid) == 0  {
		 return nil, errors.New("empty entity slice for creating")
	}
	if len(cid) != len(op) || len(cid) != len(ndl) {
	 	return nil, errors.New("wrong dimensions")	
	}
	
	ret := make([][]int, len(cid))
	blk := c.block
	for blk != nil {
		for i := 0; i < len(cid); i ++ {	
			r, err := blk.columns[cid[i]].filter(op[i], ndl[i])
			if err != nil {
				return nil, err
			}
			if ret[i] == nil {
				ret[i] = make([]int, 0, 1024)
			}
			ret[i] = append(ret[i], r...)
		}
		blk = blk.next
	}
	return ret, nil
}

func (c *columnFamily) delete(tuplePos []int) error {
	if len(tuplePos) == 0  {
		 return errors.New("empty entity slice for creating")
	}	
	blk := c.block
	for blk != nil {
		for cid, _ := range c.def {
			if err := blk.columns[cid].delete(tuplePos); err != nil {
				return err
			}
		}
		blk = blk.next
	}
	return nil
}

func (c *columnFamily) read(cid []int, tuplePos [][]int) ([]interface{}, error) {
	if len(cid) == 0  {
		 return nil, errors.New("empty entity slice for creating")
	}

	var (
		// ret interface{}
		err error
	)
	ret := make([]interface{}, len(cid))
	blk := c.block
	for blk != nil {
		for i := 0; i < len(cid); i ++ {	
			ret[i], err = blk.columns[cid[i]].read(tuplePos[i], ret[i])
			if err != nil {
				return nil, err
			}
		}
		blk = blk.next
	}
	return ret, nil
}

func (c *columnFamily) create(cid []int, e []interface{}) error {
	if len(cid) == 0  {
		 return fmt.Errorf("empty entity slice for creating (%v)", cid)//errors.New("empty entity slice for creating")
	}
	if len(cid) != len(c.def) {
	 	return errors.New("wrong dimensions")	
	}
	if len(cid) != len(e) {
	 	return errors.New("wrong dimensions")	
	}

	blk := c.block
	for blk.next !=nil {
		blk = blk.next
	}
	// var err error
	nospace := false
	for i := 0; i < len(cid); i ++ {
		if err := blk.columns[cid[i]].inspect(e[i]); err != nil {
			if err != errColDataFull {
				return err
			}
			nospace = true
		}
	}
	if nospace {
		blk.next = newColumnBlock(c.def, blk)
		blk = blk.next
	}
	for i := 0; i < len(cid); i ++ {
		if err := blk.columns[cid[i]].create(e[i]); err != nil {
			return err
		}
	}
	return nil
}

func (c *columnFamily) String() string {
	s := fmt.Sprintf("\nfamily")
	s = s + fmt.Sprintf("%v", c.block)

	b :=c.block 
	for b.next != nil {
		b = b.next
		s = s + fmt.Sprintf("\n%v", b)	
	}
	return s
}



type columnBlock struct {
	dataStore []byte
	columns map[int]columnInterface
	next *columnBlock
}
func newColumnBlock(c map[int]columnType, prev *columnBlock) *columnBlock {
	cb := &columnBlock{dataStore:make([]byte, 256*1024)/*, dim:3*/, columns:make(map[int]columnInterface)}
	dataPart := len(cb.dataStore)/len(c)
	start := 0
	end := dataPart
	for cid, ctype := range c {
		offset := 0
		if prev != nil {
			offset = prev.columns[cid].len()
		}
		switch ctype {
		case BOOL:
			cb.columns[cid] = newBoolColumn(cid, cb.dataStore[start:end:end], offset)
		case INTEGER:
			cb.columns[cid] = newIntegerColumn(cid, cb.dataStore[start:start:end], offset)
		case FLOAT:
			cb.columns[cid] = newFloatColumn(cid, cb.dataStore[start:start:end], offset)
		case TIME:
			cb.columns[cid] = newTimeColumn(cid, cb.dataStore[start:start:end], offset)
		default:
			cb.columns[cid] = newBytesliceColumn(cid, compareBytes, cb.dataStore[start:start:end], offset)
		}	
		start = end
		end += dataPart
	}
	return cb
}

type columnInterface interface {
	create(e interface{}) error 
	read(tuplePos []int, r interface{}) (interface{}, error)
	delete(tuplePos []int) error 
	filter(op relOp, n interface{}) ([]int, error) 
	len() int
	inspect(interface{}) error
}

