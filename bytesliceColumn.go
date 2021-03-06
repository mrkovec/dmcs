package dmcs

import (
	// "log"
	"sort"
	"fmt"
	"errors"
	"bytes"
)

var (
	errWrongDataType = errors.New("bytesliceColumn operates only on []byte data")
)

func compareBytes(a, b []byte) int {
	return bytes.Compare(a, b) 
}


type bytesliceColumn struct {
	id int
	tuplePosOffset int
	cf compareFunc
	byteData []byte
	entity [][2]uint32
 	
 	entityOrder []int

 	// newEnt [][]byte
 	// trxa *trxAnchor
}

func (c *bytesliceColumn) filter(op relOp, n interface{}) ([]int, error) {
	if op == ALL {
		return c.entityOrder, nil
	}

	// ndl, ok := n.([]byte)
	// if !ok {
	// 	return nil, errWrongDataType
	// }	

	// if err := c.inspect(nil, nil, op, n); err != nil {
	// 	if err == errNeedleIsLess || err == errNeedleIsGreater {
	// 		return  []int{}, nil
	// 	}
	// 	return nil, err
	// }
	ndl := n.([]byte)
	// if !ok {
	// 	return nil, fmt.Errorf("bytesliceColumn operates only on []byte data (%v) %T", c.id, n)
	// }

	// if len(ndl) == 0 {
	// 	return nil, errors.New("invalid parameter")
	// }

	sti, eni := 0, 0//len(c.entityOrder), len(c.entityOrder)
	e, wid, err := c.search(ndl)
	if err != nil {
		return nil, err
	}
	// }
	if e {
		sti = wid
		eni = wid + 1
		for eni < len(c.entityOrder) && c.cf(c.byteData[c.entity[c.entityOrder[eni]][0]:c.entity[c.entityOrder[eni]][1]], ndl) == 0 {
			eni++
		}

		switch op {
		case EQUAL:
			tuplePos := make([]int, eni-sti)
			copy(tuplePos, c.entityOrder[sti:eni])
			if c.tuplePosOffset > 0 {
				for i := 0; i < len(tuplePos); i++ {
					tuplePos[i] += c.tuplePosOffset
				}		
			}
			return tuplePos, nil
		case NOTEQUAL:
			tuplePos := make([]int, sti+len(c.entityOrder)-eni)
			copy(tuplePos, c.entityOrder[:sti])
			copy(tuplePos[sti:], c.entityOrder[eni:])
			if c.tuplePosOffset > 0 {
				for i := 0; i < len(tuplePos); i++ {
					tuplePos[i] += c.tuplePosOffset
				}		
			}
			return tuplePos, nil
		case LESS:		
			tuplePos := make([]int, sti)
			copy(tuplePos, c.entityOrder[:sti])
			if c.tuplePosOffset > 0 {
				for i := 0; i < len(tuplePos); i++ {
					tuplePos[i] += c.tuplePosOffset
				}		
			}
			return tuplePos, nil
		case LESSEQUAL:
			tuplePos := make([]int, eni)
			copy(tuplePos, c.entityOrder[:eni])
			if c.tuplePosOffset > 0 {
				for i := 0; i < len(tuplePos); i++ {
					tuplePos[i] += c.tuplePosOffset
				}		
			}
			return tuplePos, nil
		case GREATER:		
			tuplePos := make([]int, len(c.entityOrder)-eni)
			copy(tuplePos, c.entityOrder[eni:])
			if c.tuplePosOffset > 0 {
				for i := 0; i < len(tuplePos); i++ {
					tuplePos[i] += c.tuplePosOffset
				}		
			}
			return tuplePos, nil	
		case GREATEREQUAL:		
			tuplePos := make([]int, len(c.entityOrder)-sti)
			copy(tuplePos, c.entityOrder[sti:])
			if c.tuplePosOffset > 0 {
				for i := 0; i < len(tuplePos); i++ {
					tuplePos[i] += c.tuplePosOffset
				}		
			}
			return tuplePos, nil					
		default:
			return nil, errors.New("unknown operator")
		}
	}
	//not exists
	
	switch op {
	case EQUAL:
		tuplePos := make([]int, 0)
		return tuplePos, nil
	case NOTEQUAL:
		tuplePos := make([]int, len(c.entityOrder))
		copy(tuplePos, c.entityOrder)
		if c.tuplePosOffset > 0 {
			for i := 0; i < len(tuplePos); i++ {
				tuplePos[i] += c.tuplePosOffset
			}		
		}
		return tuplePos, nil
	case LESS, LESSEQUAL:		
		tuplePos := make([]int, wid)
		copy(tuplePos, c.entityOrder[:wid])
		if c.tuplePosOffset > 0 {
			for i := 0; i < len(tuplePos); i++ {
				tuplePos[i] += c.tuplePosOffset
			}		
		}
		return tuplePos, nil
	case GREATER, GREATEREQUAL:		
		tuplePos := make([]int, len(c.entityOrder)-wid)
		copy(tuplePos, c.entityOrder[wid:])
		if c.tuplePosOffset > 0 {
			for i := 0; i < len(tuplePos); i++ {
				tuplePos[i] += c.tuplePosOffset
			}		
		}
		return tuplePos, nil		
	default:
		return nil, errors.New("unknown operator")
	}

}

func (c *bytesliceColumn) delete(tuplePos []int) error {
	for i := 0; i < len(tuplePos); i++ {
		ti := tuplePos[i] - c.tuplePosOffset
		if ti >= 0 && ti < len(c.entity) {
			for j:=0; j < len(c.entityOrder); j++ {
				if c.entityOrder[j] == ti {
					c.entityOrder = c.entityOrder[:j+copy(c.entityOrder[j:], c.entityOrder[j+1:])]
					break
				}
			}
		}
	}
	return nil
}

func (c *bytesliceColumn) read(tuplePos []int, r interface{}) (interface{}, error) {
	var (
		ret [][]byte
		ok bool
	)

	if r != nil {
		ret, ok = r.([][]byte)
		if !ok {
			return nil, errWrongDataType
		}
	} else {
		ret = make([][]byte, len(tuplePos))
	}

	
	// if len(tuplePos) == 0 {
	// 	return nil, errors.New("empty tuplePos slice for reading")
	// }
	// // ret := make([][]byte, 0, len(tuplePos))
	for i := 0; i < len(tuplePos); i++ {
		ti := tuplePos[i] - c.tuplePosOffset
		if ti >= 0 && ti < len(c.entity) {
			r:= make([]byte, len(c.byteData[c.entity[ti][0]:c.entity[ti][1]]))
			// r:= make([]byte, len(c.byteData[c.entity[ti].a:c.entity[ti].gb()]))
			copy(r, c.byteData[c.entity[ti][0]:c.entity[ti][1]])
			// copy(r, c.byteData[c.entity[ti].a:c.entity[ti].gb()])
			ret[i] = r
		}
	}
	return ret, nil
}

func (c *bytesliceColumn) inspect(e interface{}, tuplePos []int, op relOp, ndl interface{}) error {
	// log.Println("inspect", c)
	if e != nil {
		ent := e.([][]byte)
		s := 0
		for i := 0; i < len(ent); i ++ {
			s += len(ent[i])
		}
		if len(c.byteData) + s > cap(c.byteData)  {
			return errColumnDataFull
		}
	}
	if tuplePos != nil {
		o := false
		for i := 0; i < len(tuplePos); i++ {
			ti := tuplePos[i] - c.tuplePosOffset
			if ti >= 0 && ti < len(c.entity) {
				o = true
			}
		}
		if !o {
			return errNoTupleFound
		}
	}
	if ndl != nil {
		// n := ndl.([]byte)
		n, ok := ndl.([]byte)
		if !ok {
			return errWrongDataType
		}	
		if op == ALL {
			return nil
		}
		if c.cf(c.byteData[c.entity[c.entityOrder[0]][0]:c.entity[c.entityOrder[0]][1]], n) > 0 {
			if op == EQUAL || op == LESS || op == LESSEQUAL {
				return errNeedleIsLess
			}
			// return errNeedleIsLess
		} else 	if c.cf(c.byteData[c.entity[c.entityOrder[len(c.entityOrder)-1]][0]:c.entity[c.entityOrder[len(c.entityOrder)-1]][1]], n) < 0 {
			if op == EQUAL || op == GREATER || op == GREATEREQUAL {
				return errNeedleIsGreater
			}
		}
	}
	return nil
}

func (c *bytesliceColumn) create(e interface{}) error {

	ent, ok := e.([][]byte)
	if !ok {
		return errWrongDataType
	}	

	/*if err := c.inspect(e, nil, 0, nil); err != nil {
			return err
	}*/


	// ent := c.newEnt//e.([][]byte)
	

	// log.Println(ent)
	// if !ok {
	// 	return errors.New("bytesliceColumn operates only on []byte data")
	// }	
	// if len(ent) == 0 {
	// 	return errors.New("empty entity slice for creating")
	// }
	// if !c.hasSpace(e) {
	// 	return errColDataFull
	// }
	tuplePos := len(c.entity)	
	// if tuplePos + len(ent) > cap(c.byteData) {
	// 	return errColDataFull
	// }
	// log.Println(tuplePos, len(ent), cap(c.byteData), cap(c.entity))
	
	//c.entity = c.entity[:tuplePos + len(ent)]
	c.entity = append(c.entity, make([][2]uint32, len(ent))...)

	for i := 0; i < len(ent); i++ {
		n := len(c.byteData)
		ex, wid, err := c.search(ent[i])
		if err != nil {
			return err
		}		
		//c.entityOrder = c.entityOrder[:len(c.entityOrder) + 1]
		c.entityOrder = append(c.entityOrder, 0)

		copy(c.entityOrder[wid + 1:], c.entityOrder[wid:])
		if ex {
			// c.entity[tuplePos].body = c.entity[c.entityOrder[wid+1]].body
			c.entity[tuplePos][0], c.entity[tuplePos][1] = c.entity[c.entityOrder[wid+1]][0], c.entity[c.entityOrder[wid+1]][1]
			// c.entity[tuplePos].a, c.entity[tuplePos].b = c.entity[c.entityOrder[wid+1]].a, c.entity[c.entityOrder[wid+1]].b
			c.entityOrder[wid] = tuplePos			
		} else {
			c.byteData = append(c.byteData, ent[i]...)			
			c.entity[tuplePos][0], c.entity[tuplePos][1]  = uint32(n), uint32( n + len(ent[i]))
			c.entityOrder[wid] = tuplePos	
		}
		tuplePos++
		//n += len(ent[i])		
	}
	return nil
}

func (c *bytesliceColumn) search(ndl []byte) (bool, int, error) {
	if c.cf == nil {
	 	return false, len(c.entityOrder), nil
	}
	//i := sort.Search(len(c.entityOrder), func(i int) bool { return  c.cf(c.byteData[int(c.entity[c.entityOrder[i]][0]):int(c.entity[c.entityOrder[i]][1])], ndl) >= 0 })
	i := sort.Search(len(c.entityOrder), func(i int) bool { 
		a := int(c.entity[c.entityOrder[i]][0])
		b := int(c.entity[c.entityOrder[i]][1])
		// log.Println(a, b, len(c.byteData))
		return  c.cf(c.byteData[a:b], ndl) >= 0 
	})
	if i < len(c.entityOrder) && c.cf(c.byteData[c.entity[c.entityOrder[i]][0]:c.entity[c.entityOrder[i]][1]], ndl) == 0 {
		return true, i, nil
	} else {
		return false, i, nil
	}
}

func (c *bytesliceColumn) len() (int, int) {
	return len(c.entityOrder), len(c.entityOrder) + c.tuplePosOffset
}


func (c *bytesliceColumn) String() string {
	// var s string
	// s = fmt.Sprintf("\ncol[%v] of:%v [%vB]", c.id, c.tuplePosOffset, cap(c.byteData))
	// for tuplePos, h := range c.entity {
	// 	s = s + fmt.Sprintf("\n\t[%v]ent %v", tuplePos + c.tuplePosOffset, c.byteData[h[0]:h[1]])
	// }
	// s = s + fmt.Sprintf("\n\torder:%v", c.entityOrder)
	// return s
	return fmt.Sprintf("[%v] %v (%v)", c.id, statNumber(len(c.entityOrder)), byteSize(c.size()))
}

func (c *bytesliceColumn) size() int {
	return cap(c.byteData) + cap(c.entity)*64 + cap(c.entityOrder)*64
}

func newBytesliceColumn(id int, cf compareFunc, storage []byte, offset int) *bytesliceColumn {
	return &bytesliceColumn{id: id,
		tuplePosOffset: offset,
		cf: cf,
		byteData: storage,
		entity: make([][2]uint32, 0, 64*1024),		
		entityOrder: make([]int, 0, 64*1024)}
}
func newBytesliceColumn2(id ColumnId, storage []byte, offset int) columnInterface {
	return &bytesliceColumn{id: int(id),
		tuplePosOffset: offset,
		cf: compareBytes,
		byteData: storage,
		entity: make([][2]uint32, 0, 1024),		
		entityOrder: make([]int, 0, 1024)}
}



// func (c *boolColumn) filter(op relOp, n interface{}) ([]int, error) {
// 	if op == ALL {
// 		return c.entityOrder, nil
// 	}

// 	ndl, ok := n.(bool)
// 	if !ok {
// 		return nil, errors.New("boolColumn operates only on bool data")
// 	}

// 	sti, eni := 0, 0
// 	e, wid, err := c.search(ndl)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if e {
// 		sti = wid
// 		if ndl {
// 			eni = len(c.entityOrder)	
// 		} else {
// 			eni = c.firstTrue	
// 		}
// 	}
// 	switch op {
// 	case EQUAL:
// 		tuplePos := make([]int, eni-sti)
// 		copy(tuplePos, c.entityOrder[sti:eni])
// 		return tuplePos, nil
// 	case NOTEQUAL:
// 		tuplePos := make([]int, sti+len(c.entityOrder)-eni)
// 		copy(tuplePos, c.entityOrder[:sti])
// 		copy(tuplePos[sti:], c.entityOrder[eni:])
// 		return tuplePos, nil
// 	default:
// 		return nil, errors.New("unknown operator")
// 	}
// }

// func (c *boolColumn) delete(tuplePos []int) error {
// 	for i := 0; i < len(tuplePos); i++ {
// 		if tuplePos[i] < 0 || tuplePos[i] >= len(c.entity) {
// 			return errors.New("wrong tuplePos")		
// 		}
// 		for j:=0; j < len(c.entityOrder); j++ {
// 			if c.entityOrder[j] == tuplePos[i] {
// 				if j == c.firstTrue {
// 					if j + 1 < len(c.entityOrder) {
// 						c.firstTrue++
// 					} else {
// 						c.firstTrue = -1
// 					}
// 				}
// 				// c.entityOrder  = append(c.entityOrder[:j], c.entityOrder[j+1:]...) 
// 				c.entityOrder = c.entityOrder[:j+copy(c.entityOrder[j:], c.entityOrder[j+1:])]
// 				break
// 			}
// 		}
// 	}
// 	return nil
// }

// func (c *boolColumn) read(tp interface{}) (interface{}, error) {
// 	tuplePos, ok := tp.([]int)
// 	if !ok {
// 		return nil, errors.New("read for boolColumn requires []int")
// 	}

// 	if len(tuplePos) == 0 {
// 		return nil, errors.New("empty tuplePos slice for reading")
// 	}
// 	ret := make([]bool, len(tuplePos))
// 	for i := 0; i < len(tuplePos); i++ {
// 		if tuplePos[i] < 0 || tuplePos[i] >= len(c.entity) {
// 			return nil, errors.New("wrong tuplePos")		
// 		}
// 		bit := (c.byteData[c.entity[tuplePos[i]]] >> uint8(tuplePos[i]%8)) & 1
// 		switch bit {
// 		case 0:
// 			ret[i] = false
// 		case 1:
// 			ret[i] = true
// 		default:
// 			return nil, errors.New("data error")			
// 		}
// 	}
// 	return ret, nil
// }

// func (c *boolColumn) create(e interface{}) error {
// 	ent, ok := e.([]bool)
// 	if !ok {
// 		return errors.New("boolColumn operates only on bool data")
// 	}
// 	if len(ent) == 0 {
// 		return errors.New("empty entity slice for creating")
// 	}

// 	tuplePos := len(c.entity)	
// 	c.entity = c.entity[:len(c.entity) + len(ent)]

// 	for i := 0; i < len(ent); i++ {
// 		bytenum := tuplePos/8
// 		bitnum := uint8(tuplePos%8)
// 		c.entity[tuplePos] = bytenum

// 		_, wid, err := c.search(ent[i])
// 		if err != nil {
// 			return err
// 		}
// 		// log.Println(wid)
// 		c.entityOrder = c.entityOrder[:len(c.entityOrder) + 1]
// 		copy(c.entityOrder[wid + 1:], c.entityOrder[wid:])
// 		c.entityOrder[wid] = tuplePos

// 		if ent[i] {
// 			c.byteData[bytenum] |= 1 << bitnum
// 			if c.firstTrue < 0 {
// 				c.firstTrue = wid
// 			}
// 		} else {
// 			c.byteData[bytenum] &= ^(1 << bitnum)
// 			if c.firstTrue >= 0 {
// 				c.firstTrue++
// 			}			
// 		}
//  		tuplePos++
// 	}
// 	return nil
// }

// func (c *boolColumn) search(n interface{}) (bool, int, error) {
// 	ndl, ok := n.(bool)
// 	if !ok {
// 		return false, -1, errors.New("boolColumn operates only on bool data")
// 	}

// 	if ndl {
// 		if c.firstTrue < 0 {
// 			return false, len(c.entityOrder), nil
// 		}
// 		return true, c.firstTrue, nil
// 	}
// 	if c.firstTrue == 0 {
// 		return false, 0, nil
// 	}
// 	return true, 0, nil
// }

// func (c *boolColumn) String() string {
// 	s := fmt.Sprintf("\n[%v] ft:%v ls:%v/lx:%v", c.id, c.firstTrue, c.ls, c.lx)
// 	for tuplePos, h := range c.entity {
// 		s = s + fmt.Sprintf("\n\t[%v]entity %b (%v)", tuplePos, c.byteData[h], ((c.byteData[h] >> uint8(tuplePos%8)) & 1)==1 )
// 	}
// 	s = s + fmt.Sprintf("\n\torder:%v", c.entityOrder)
// 	return s
// }

