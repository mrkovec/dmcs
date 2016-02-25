package dmcs

import (
	"errors"
)
var (
	errWrongState = errors.New("wrong transaction state")
)
/*
*/
type trxGovernor struct {
	trxQueue []*transaction
}

type trxState int
const (
	invalid trxState = iota
	free 
	blocked
	running
	finished
)

/*
*/
type transaction struct {
	state trxState
	body trxBody
	domain *columnFamily

	isInspect bool
	exclusiveSet map[*columnBlock]struct{}
	sharedSet map[*columnBlock]struct{}
	// createSet map[*columnBlock]struct{}
	// readSet map[*columnBlock]struct{}
	// deleteSet map[*columnBlock]struct{}
	// filterSet map[*columnBlock]struct{}
}

func (t *transaction) Filter(cid []ColumnId, op []relOp, ndl []interface{}) ([][]int, error) {
	if !t.isInspect {
		return t.domain.Filter(cid, op, ndl)
	}
	//inspect and fill createset for transaction
	_, _, _, blk, err := t.domain.inspectFilter(cid, op, ndl)
	if err != nil {
		return  nil, err
	}
	for i := 0; i < len(blk); i++ {
		_, ex := t.sharedSet[blk[i]]
		if !ex {	
			 t.sharedSet[blk[i]] = struct{}{}
			 if blk[i].trxa.locks() {
			 	t.state = blocked
			 }
		}
	}
	return nil, nil
}
func (t *transaction) Delete(tuplePos []int) error {
	if !t.isInspect {
		return t.domain.Delete(tuplePos)
	}
	//inspect and fill createset for transaction
	_, blk, err := t.domain.inspectDelete(tuplePos) 
	if err != nil {
		return  err
	}
	for i := 0; i < len(blk); i++ {
		_, ex := t.exclusiveSet[blk[i]]
		if !ex {	
			 t.exclusiveSet[blk[i]] = struct{}{}
			 if blk[i].trxa.lockx() {
			 	t.state = blocked
			 }
		}
	}
	return nil
}
func (t *transaction) Read(cid []ColumnId, tuplePos [][]int) ([]interface{}, error) {
	if !t.isInspect {
		return t.domain.Read(cid, tuplePos)
	}
	//inspect and fill createset for transaction
	_, _, blk, err := t.domain.inspectRead(cid, tuplePos) 
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(blk); i++ {
		_, ex := t.sharedSet[blk[i]]
		if !ex {	
			 t.sharedSet[blk[i]] = struct{}{}
			 if blk[i].trxa.locks() {
			 	t.state = blocked
			 }
		}
	}
	return nil, nil
}
func (t *transaction) Create(cid []ColumnId, e []interface{}) error {
	if !t.isInspect {
		return t.domain.create(t.domain.inspectCreate(cid, e))
	}
	//inspect and fill createset for transaction
	_, _, blk, err := t.domain.inspectCreate(cid, e) 
	if err != nil {
		return err
	}
	_, ex := t.exclusiveSet[blk]
	if !ex {	
		 t.exclusiveSet[blk] = struct{}{}
		 if blk.trxa.lockx() {
		 	t.state = blocked
		 }
	}
	return nil
}
func (t *transaction) finish(err error) error {
	for blk, _ := range t.exclusiveSet {
		blk.trxa.unlockx()
	}
	for blk, _ := range t.sharedSet {
		blk.trxa.unlocks()
	}
	return err
}
func (t *transaction) run() error {
	if t.state != free {
		return errWrongState
	}
	t.isInspect = false
	return t.finish(t.body(t))
}
func (t *transaction) inspect() error {
	if t.state == finished {
		return errWrongState
	}	
	t.isInspect = true
	t.state = free
	return t.body(t)
}
// func (t *transaction) exec() error {
//  	return t.body(t)
// }

func newTransaction(b trxBody, d *columnFamily) (*transaction, error) {
	trx := &transaction{body: b, 
						domain:d, 
						isInspect: true, 
						exclusiveSet: make(map[*columnBlock]struct{}),
						sharedSet: make(map[*columnBlock]struct{})}
	return trx, nil
}

/*
*/
type trxBody func(*transaction) error

/*
*/
type trxAnchor struct {
	ls uint32
	lx uint32
}
func newTrxAnchor() *trxAnchor {
	return &trxAnchor{0,0}
}
func (ta *trxAnchor) locks() bool {
	ta.ls++
	return ta.lx > 0
}
func (ta *trxAnchor) unlocks() {
	ta.ls--
}
func (ta *trxAnchor) lockx() bool {
	ta.lx++
	return ta.lx > 1 || ta.ls > 0
}
func (ta *trxAnchor) unlockx() {
	ta.lx--
}