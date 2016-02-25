package dmcs

import (
	"log"
	"fmt"
	"errors"
)

var (
	ByteSlice = newBytesliceColumn2
)


const (
	DefaultBlockSize = 256*1024
)
var (
	BlockSize = DefaultBlockSize
)

var (
	errColumnDataFull = errors.New("column data full")
	errNoTupleFound = errors.New("tuple not found")
	errNeedleIsGreater = errors.New("needle not found")
	errNeedleIsLess = errors.New("needle not found")
	errWrongDim = errors.New("wrong dimensions")
)


type columnFamily struct {
	def map[ColumnId]ColumnType
	block *columnBlock
	log bool
}

// func NewTESTColumnFamily() *columnFamily {
// 	return newColumnFamily(map[int]columnType{0:INTEGER})
// }

// func newColumnFamily(c map[int]columnType) *columnFamily {
// 	return &columnFamily{def:c, block:newColumnBlock(c, nil)}
// }
func NewColumnFamily(c map[ColumnId]ColumnType) *columnFamily {
	return &columnFamily{def:c, block:newColumnBlock(c, nil, nil), log:false}
}
func NewColumnFamilyLogged(c map[ColumnId]ColumnType) *columnFamily {
	return &columnFamily{def:c, block:newColumnBlock(c, nil, newTrxAnchor()), log:true}
}

func (c *columnFamily) inspectFilter(cid []ColumnId, op []relOp, ndl []interface{})  ([]ColumnId, []relOp, []interface{}, []*columnBlock, error) {
	if len(cid) == 0  {
		 return nil, nil, nil, nil, errWrongDim
	}
	if len(cid) != len(op) || len(cid) != len(ndl) {
	 	return nil, nil, nil, nil, errWrongDim
	}

	retb := make([]*columnBlock, 0, 64)
	fnd := false	
	blk := c.block
	for blk != nil {
		fnd = false
		for i := 0; i < len(cid); i ++ {	
			if err := blk.columns[cid[i]].inspect(nil, nil, op[i], ndl[i]); err != nil {
				if err != errNeedleIsLess && err != errNeedleIsGreater {
					return nil, nil, nil, nil, err	
				}
			} else {
				fnd = true
			}
		}
		if fnd {
			retb = append(retb, blk)
		}		
		blk = blk.next
	}	
	return cid, op, ndl, retb, nil

}

func (c *columnFamily) Filter(cid []ColumnId, op []relOp, ndl []interface{}) ([][]int, error) {
	return c.filter(c.inspectFilter(cid, op, ndl))
}

func (c *columnFamily) filter(cid []ColumnId, op []relOp, ndl []interface{}, blk []*columnBlock, err error) ([][]int, error) {
	// blk, err := c.inspectFilter(cid, op, ndl)
	if err != nil {
		return nil, err
	}
	// log.Println(blk)
	ret := make([][]int, len(cid))
	for j := 0; j < len(blk); j++ {
		for i := 0; i < len(cid); i ++ {	
			r, err := blk[j].columns[cid[i]].filter(op[i], ndl[i])
			if err != nil {
				return nil, err
			}
			if ret[i] == nil {
				ret[i] = make([]int, 0, 1024)
			}
			ret[i] = append(ret[i], r...)
		}
	}
	return ret, nil
}
func (c *columnFamily) inspectDelete(tuplePos []int) ([]int, []*columnBlock, error) {
	if len(tuplePos) == 0  {
		 return nil, nil, errWrongDim
	}	
	retb := make([]*columnBlock, 0, 64)
	fnd := false
	blk := c.block
	for blk != nil {
		fnd = false
		for cid, _ := range c.def {
			if err := blk.columns[cid].inspect(nil, tuplePos, 0, nil); err != nil {
				if err != errNoTupleFound {
					return nil, nil, err	
				}
			} else {
				fnd = true
			}
		}
		if fnd {
			retb = append(retb, blk)
		}
		blk = blk.next
	}
	return tuplePos, retb, nil
}

func (c *columnFamily) Delete(tuplePos []int) error {
	return c.delete(c.inspectDelete(tuplePos))
}

func (c *columnFamily) delete(tuplePos []int, blk []*columnBlock, err error) error {
 	// blk, err := c.inspectDelete(tuplePos)
	if err != nil {
		return err
	}

	for j := 0; j < len(blk); j++ {
		for cid, _ := range c.def {
			if err := blk[j].columns[cid].delete(tuplePos); err != nil {
				return err
			}
		}
	}
	return nil
}
func (c *columnFamily) inspectRead(cid []ColumnId, tuplePos [][]int) ([]ColumnId, [][]int, []*columnBlock, error) {
	if len(cid) == 0  {
		 return nil, nil, nil, errWrongDim
	}
	retb := make([]*columnBlock, 0, 64)
	blk := c.block
	fnd := false
	for blk != nil {
		fnd = false
		for i := 0; i < len(cid); i ++ {	
			err := blk.columns[cid[i]].inspect(nil, tuplePos[i], 0, nil)
			if err != nil {
				if err != errNoTupleFound {
					return nil, nil, nil, err	
				}
			} else {
				fnd = true
			}
		}
		if fnd {
			retb = append(retb, blk)
		}
		blk = blk.next
	}
	return cid, tuplePos, retb, nil
}

func (c *columnFamily) Read(cid []ColumnId, tuplePos [][]int) ([]interface{}, error) {
	return c.read(c.inspectRead(cid, tuplePos))
}

func (c *columnFamily) read(cid []ColumnId, tuplePos [][]int, blk []*columnBlock, err error) ([]interface{}, error) {
 	// blk, err := c.inspectRead(cid, tuplePos)
	if err != nil {
		return nil, err
	}
	// log.Println(blk)
	ret := make([]interface{}, len(cid))
	for j := 0; j < len(blk); j++ {
		for i := 0; i < len(cid); i ++ {	
			ret[i], err = blk[j].columns[cid[i]].read(tuplePos[i], ret[i])
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, nil
}
func (c *columnFamily) inspectCreate(cid []ColumnId, e []interface{}) ([]ColumnId, []interface{}, *columnBlock,  error) {
	if len(cid) == 0  {
		 return nil, nil, nil, errWrongDim
	}
	if len(cid) != len(c.def) {
	 	return nil, nil, nil, errWrongDim
	}
	if len(cid) != len(e) {
	 	return nil, nil, nil, errWrongDim
	}
	// a := len(e[0])
	// for i := 1; i < len(e); i ++ {
	// 	if a != len(e[i]) {
	// 		return errWrongDim
	// 	}
	// 	a = len(e[i])
	// }
	//locking enabled
	blk := c.block
	for blk.next  != nil {
		blk = blk.next
	}
	for i := 0; i < len(cid); i ++ {
		if err := blk.columns[cid[i]].inspect(e[i], nil, 0, nil); err != nil {
			if err != errColumnDataFull {
				return nil, nil, nil, err
			}
			return cid, e, blk, err
		}
	}	
	return cid, e, blk, nil
}

func (c *columnFamily) Create(cid []ColumnId, e []interface{}) error {
	return c.create(c.inspectCreate(cid, e))
}
func (c *columnFamily) create(cid []ColumnId, e []interface{}, blk *columnBlock, err error) error {	
	// blk, err := c.inspectCreate(cid, e)
	if err != nil && err != errColumnDataFull {
		return err
	}

	// blk := c.block
	// for blk.next !=nil {
	// 	blk = blk.next
	// }
	// var err error
	// nospace := false
	// for i := 0; i < len(cid); i ++ {
	// 	if err := blk.columns[cid[i]].inspect(e[i], nil, 0, nil); err != nil {
	// 		// log.Println(err)
	// 		if err != errColumnDataFull {
	// 			return err
	// 		}
	// 		nospace = true
	// 	}
	// }
	// if nospace {
	if err == errColumnDataFull {
		if c.log {
			blk.next = newColumnBlock(c.def, blk, newTrxAnchor())	
		} else {
			blk.next = newColumnBlock(c.def, blk, nil)	
		}
		blk = blk.next
	}
	
	for i := 0; i < len(cid); i ++ {
		if err := blk.columns[cid[i]].create(e[i]); err != nil {
			return err
		}
	}	

	return nil
}

func (c *columnFamily) len() int {
	blk := c.block
	i := 0
	for blk !=nil {
		blk = blk.next
		i++
	}
	return i
}

func (c *columnFamily) String() string {
	// blk := c.block
	// i := 0
	// sum := 0
	// size := 0
	// for blk !=nil {
	// 	for _, cl := range blk.columns {
	// 		n, _:= cl.len()
	// 		sum += n
	// 		size += cl.size()
	// 	}
	// 	blk = blk.next
	// 	i++
	// }

	// s := fmt.Sprintf("\nfamily blocks:%v - %v [%v]", i, sum, size)
	// // s = s + fmt.Sprintf("%v", c.block)

	// b :=c.block 
	// for b  != nil {
	// 	s = s + fmt.Sprintf("\n%v", b)	
	// 	b = b.next
	// }
	// return s
	// cd := ""
	// dt := make(map[ColumnId][2]int)

	blk := c.block
	i := 0
	n := 0
	s := 0
	cs := ""
	for blk !=nil {
		for _, cl := range blk.columns {
			tn, _:= cl.len()
			n +=tn
			s +=cl.size()
			
		}
		cs += fmt.Sprintf("\n%v", blk)
		blk = blk.next
		i++
	}
	return fmt.Sprintf("[%p] %v(%v); %v blocks:%v", c, statNumber(n), byteSize(s), statNumber(i), cs)
}

 

func (c *columnBlock) String() string {
	// s := fmt.Sprintf("\nblk %p", c)
	// for _, cl := range c.columns {
	// 	n, _ := cl.len()
	// 	s = s + fmt.Sprintf("\n %v [%v]", n, cl.size()/1024)
	// 	// s = s + fmt.Sprintf("\n %v", cl)
	// }
	cs := ""
	n := 0
	s := 0
	for _, cl := range c.columns {
		//tn, _ := cl.len()
		n++ 
		s += cl.size()
		cs = cs + "\n" + fmt.Sprintf("%v", cl)
		// s = s + fmt.Sprintf("\n %v", cl)
	}
	return fmt.Sprintf("[%p] %v; %v; %v columns:%v", c, byteSize(s), statNumber(n), c.trxa, cs)
}

type columnBlock struct {
	dataStore []byte
	// columns map[int]columnInterface
	columns map[ColumnId]columnInterface
	next *columnBlock

	trxa *trxAnchor
}
// func newColumnBlock(c map[int]columnType, prev *columnBlock) *columnBlock {
// 	cb := &columnBlock{dataStore:make([]byte, 256*1024)/*, dim:3*/, columns:make(map[int]columnInterface)}
// 	dataPart := len(cb.dataStore)/len(c)
// 	start := 0
// 	end := dataPart
// 	for cid, ctype := range c {
// 		offset := 0
// 		if prev != nil {
// 			offset = prev.columns[cid].len()
// 		}
// 		switch ctype {
// 		// case BOOL:
// 		// 	cb.columns[cid] = newBoolColumn(cid, cb.dataStore[start:end:end], offset)
// 		case INTEGER:
// 			cb.columns[cid] = newIntegerColumn(cid, cb.dataStore[start:start:end], offset)
// 		// case FLOAT:
// 		// 	cb.columns[cid] = newFloatColumn(cid, cb.dataStore[start:start:end], offset)
// 		// case TIME:
// 		// 	cb.columns[cid] = newTimeColumn(cid, cb.dataStore[start:start:end], offset)
// 		default:
// 			cb.columns[cid] = newBytesliceColumn(cid, compareBytes, cb.dataStore[start:start:end], offset)
// 		}	
// 		start = end
// 		end += dataPart
// 	}
// 	return cb
// }

func newColumnBlock(c map[ColumnId]ColumnType, prev *columnBlock, ta *trxAnchor) *columnBlock {
	cb := &columnBlock{dataStore:make([]byte, BlockSize), 
					columns:make(map[ColumnId]columnInterface),
					trxa: ta}

	dataPart := len(cb.dataStore)/len(c)
	start := 0
	end := dataPart
	for cid, ctype := range c {
		offset := 0
		if prev != nil {
			_, offset = prev.columns[cid].len()
			// log.Println("offset", offset)
		}
		cb.columns[cid] = ctype(cid, cb.dataStore[start:start:end], offset)
		start = end
		end += dataPart
	}
	return cb
}

type columnInterface interface {
	create(e interface{}) error 
	read(tuplePos []int, r interface{}) (interface{}, error)
	delete(tuplePos []int) error 
	filter(op relOp, ndl interface{}) ([]int, error) 
	len() (int, int)
	size() int
	inspect(e interface{}, tuplePos []int, op relOp, ndl interface{}) error
}

 
type ColumnId int
type ColumnType func(ColumnId, []byte, int) columnInterface
type compareFunc func([]byte, []byte) int 

func init() {
	// log.SetOutput(os.Stdout)
	log.SetFlags(log.Lshortfile)
	//if !showlog {
		//log.SetOutput(ioutil.Discard)
	//}
}