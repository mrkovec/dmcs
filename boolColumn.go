package dmcs

import (
	// "log"
	"fmt"
	"errors"
)

type boolColumn struct {
	id int
	tuplePosOffset int
	byteData []byte
	entity []int
 	
 	entityOrder []int	
 	firstTrue int

	ls int
	lx int
}

func (c *boolColumn) filter(op relOp, n interface{}) ([]int, error) {
	if op == ALL {
		return c.entityOrder, nil
	}
	ndl, ok := n.(bool)
	if !ok {
		return nil, errors.New("boolColumn operates only on bool data")
	}

	sti, eni := 0, 0
	e, wid, err := c.search(ndl)
	if err != nil {
		return nil, err
	}
	if e {
		sti = wid
		if ndl {
			eni = len(c.entityOrder)	
		} else {
			eni = c.firstTrue	
		}
	}
	switch op {
	case EQUAL:
		tuplePos := make([]int, eni-sti)
		copy(tuplePos, c.entityOrder[sti:eni])
		for i := 0; i < len(tuplePos); i++ {
			tuplePos[i] += c.tuplePosOffset
		}
		return tuplePos, nil		
	case NOTEQUAL:
		tuplePos := make([]int, sti+len(c.entityOrder)-eni)
		copy(tuplePos, c.entityOrder[:sti])
		copy(tuplePos[sti:], c.entityOrder[eni:])
		for i := 0; i < len(tuplePos); i++ {
			tuplePos[i] += c.tuplePosOffset
		}
		return tuplePos, nil		
	default:
		return nil, errors.New("unknown operator")
	}

}

func (c *boolColumn) delete(tuplePos []int) error {
	for i := 0; i < len(tuplePos); i++ {
		ti := tuplePos[i] - c.tuplePosOffset
		if ti >= 0 && ti < len(c.entity) {
			for j:=0; j < len(c.entityOrder); j++ {
				if c.entityOrder[j] == ti {
					if j == c.firstTrue {
						if j + 1 < len(c.entityOrder) {
							c.firstTrue++
						} else {
							c.firstTrue = -1
						}
					}
					// c.entityOrder  = append(c.entityOrder[:j], c.entityOrder[j+1:]...) 
					c.entityOrder = c.entityOrder[:j+copy(c.entityOrder[j:], c.entityOrder[j+1:])]
					break
				}
			}
		}
	}
	return nil
}

func (c *boolColumn) read(tuplePos []int, r interface{}) (interface{}, error) {
	// tuplePos, ok := tp.([]int)
	// if !ok {
	// 	return nil, errors.New("read for boolColumn requires []int")
	// }
	var ret []bool
	var ok bool
	if r != nil {
		ret, ok = r.([]bool)
		if !ok {
			return nil, errors.New("boolColumn operates only on bool data")
		}
	} else {
		ret = make([]bool, len(tuplePos))	
	}

	if len(tuplePos) == 0 {
		return nil, errors.New("empty tuplePos slice for reading")
	}
	// ret := make([]bool, len(tuplePos))
	for i := 0; i < len(tuplePos); i++ {
		ti := tuplePos[i] - c.tuplePosOffset
		// log.Println(i, c.tuplePosOffset, ti, len(c.entity))
		if ti >= 0 && ti < len(c.entity) {		
		// if i >= c.tuplePosOffset && i < c.tuplePosOffset + len(c.entity) {
			// ui :=  i -  c.tuplePosOffset
			// if tuplePos[ui] < 0 || tuplePos[ui] >= len(c.entity) {
			// 	return nil, errors.New("wrong tuplePos")		
			// }
			bit := (c.byteData[c.entity[ti]] >> uint8(ti%8)) & 1
			switch bit {
			case 0:
				ret[i] = false
			case 1:
				ret[i] = true
			default:
				return nil, errors.New("data error")			
			}
		}
	}
	return ret, nil
}

func (c *boolColumn) inspect(e interface{}) error {
	ent, ok := e.([]bool)
	if !ok {
		return errors.New("boolColumn operates only on bool data")
	}
	if len(ent) == 0 {
		return errors.New("empty entity slice for creating")
	}
	if (len(ent) + len(c.entity))/8 >= cap(c.byteData) {
		return errColDataFull
	}
	return nil
}


func (c *boolColumn) create(e interface{}) error {
	if err := c.inspect(e); err != nil {
		return err
	}

	ent := e.([]bool)
 
	tuplePos := len(c.entity)	
 
	c.entity = c.entity[:len(c.entity) + len(ent)]

	for i := 0; i < len(ent); i++ {
		bytenum := tuplePos/8
		bitnum := uint8(tuplePos%8)
		c.entity[tuplePos] = bytenum

		_, wid, err := c.search(ent[i])
		if err != nil {
			return err
		}
		// log.Println(wid)
		c.entityOrder = c.entityOrder[:len(c.entityOrder) + 1]
		copy(c.entityOrder[wid + 1:], c.entityOrder[wid:])
		c.entityOrder[wid] = tuplePos

		if ent[i] {
			c.byteData[bytenum] |= 1 << bitnum
			if c.firstTrue < 0 {
				c.firstTrue = wid
			}
		} else {
			c.byteData[bytenum] &= ^(1 << bitnum)
			if c.firstTrue >= 0 {
				c.firstTrue++
			}			
		}
 		tuplePos++
	}
	return nil
}

func (c *boolColumn) search(n interface{}) (bool, int, error) {
	ndl, ok := n.(bool)
	if !ok {
		return false, -1, errors.New("boolColumn operates only on bool data")
	}

	if ndl {
		if c.firstTrue < 0 {
			return false, len(c.entityOrder), nil
		}
		return true, c.firstTrue, nil
	}
	if c.firstTrue == 0 {
		return false, 0, nil
	}
	return true, 0, nil
}

func (c *boolColumn) len() int {
	return len(c.entity)
}
// func (c *boolColumn) appendData(a interface{}, b interface{}) (intreface{}, err) {
// 	return len(c.entity)
// }

func (c *boolColumn) String() string {
	s := fmt.Sprintf("\ncol[%v] ft:%v ls:%v/lx:%v of:%v [%vB]", c.id, c.firstTrue, c.ls, c.lx, c.tuplePosOffset, cap(c.byteData))
	for tuplePos, h := range c.entity {
		s = s + fmt.Sprintf("\n\t[%v]ent %b (%v)", tuplePos + c.tuplePosOffset, c.byteData[h], ((c.byteData[h] >> uint8(tuplePos%8)) & 1)==1 )
	}
	s = s + fmt.Sprintf("\n\torder:%v", c.entityOrder)
	return s
}

func newBoolColumn(id int, storage []byte, offset int) *boolColumn {
	return &boolColumn{id: id,
		tuplePosOffset: offset,
		firstTrue: -1,
		byteData: storage,
		entity: make([]int, 0, 1024),		
		entityOrder: make([]int, 0, 1024)}
}
