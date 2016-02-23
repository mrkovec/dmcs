package dmcs

/*
*/
type trxGovernor struct {
	trxQueue []*transaction
}

/*
*/
type transaction struct {
	// id int
	body trxBody
	domain *columnFamily

	inspect bool
	createSet map[*columnBlock]struct{}
	// deleteSet map[*columnBlock]struct{}
	// readSet map[*columnBlock]struct{}
	// cSet map[int][]int
	// rSet map[int]map[int]struct{}
	// wSet map[int]map[int][]byte
	// dSet map[int][]int
	// free bool
	// state trxState
}
func (t *transaction) Create(cid []ColumnId, e []interface{}) error {
	if !t.inspect {
		return nil
	}
	//inspect and fill createset for transaction
	_, _, blk, err := t.domain.inspectCreate(cid, e) 
	if err != nil {
		return nil
	}
	_, ex := t.createSet[blk]
	if !ex {	
		 t.createSet[blk] = struct{}{}
	}
	return nil

}

func newTransaction(b trxBody, d *columnFamily) (*transaction, error) {
	trx := &transaction{body: b, domain:d, inspect: true, createSet: make(map[*columnBlock]struct{})}
	if err := trx.body(trx); err != nil {
		return nil, err
	}
	trx.inspect = false
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
func (ta *trxAnchor) lockS() {
	ta.ls++
}
func (ta *trxAnchor) unlockS() {
	ta.ls--
}
func (ta *trxAnchor) lockX() {
	ta.lx++
}
func (ta *trxAnchor) unlockX() {
	ta.lx--
}