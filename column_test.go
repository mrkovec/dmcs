package dmcs

import (
	// "log"
	"testing"
	"fmt"
	"time"
)

const (
	empEmpno int = iota
	empEname
	empJob
	empMgr
	empHiredate
	empSal
	empComm
	empDeptno
)

var (
	empAllCol = []int{empEmpno, empEname, empJob, empMgr, empHiredate, empSal, empComm, empDeptno}
	empColTypes = map[int]columnType{empEmpno:INTEGER, empEname:BYTESLICE, empJob:BYTESLICE, empMgr:INTEGER, empHiredate:TIME, empSal:FLOAT, empComm:FLOAT, empDeptno:INTEGER}
)

func TestColumnFamily(t *testing.T) {
	emp := newColumnFamily(empColTypes)

	if err := emp.create(empAllCol, []interface{}{pi(7369), ps("SMITH"), ps("CLERK"), pi(7902), pt("17-DEC-1980"), pf(800), pf(0), pi(20)}); err != nil {
		t.Fatal(err)
	} 
	if err := emp.create(empAllCol, []interface{}{pi(7499), ps("ALLEN"), ps("SALESMAN"), pi(7698), pt("20-FEB-1981"), pf(1600), pf(300), pi(30)}); err != nil {
		t.Fatal(err)
	} 
	if err := emp.create(empAllCol, []interface{}{pi(7521), ps("WARD"), ps("SALESMAN"), pi(7698), pt("2-FEB-1981"), pf(1250), pf(500), pi(30)}); err != nil {
		t.Fatal(err)
	} 

	if err := emp.create(empAllCol, []interface{}{pi(7566, 7654, 7698, 7782, 7788, 7839), ps("JONES", "MARTIN", "BLAKE", "CLARK", "SCOTT", "KING"), ps("MANAGER", "SALESMAN", "MANAGER", "MANAGER", "ANALYST", "PRESIDENT"), pi(7839, 7698, 7839, 7839, 7566, 0), pt("2-APR-1981", "28-SEP-1981", "1-MAY-1981", "9-JUN-1981", "09-DEC-1982", "17-NOV-1981"), pf(2975, 1250, 2850, 2450, 3000, 5000), pf(0, 1400, 0, 0, 0, 0), pi(20, 30, 20, 10, 20, 10)}); err != nil {
		t.Fatal(err)
	} 
	 
	tuplepos, err := emp.filter([]int{empEname, empDeptno}, []relOp{EQUAL, EQUAL}, []interface{}{ps("KING")[0], pi(10)[0]})
	if err != nil {
		t.Fatal(err)
	} 
	e, g = "[8] [6 8]", fmt.Sprintf("%v %v", sortTP(tuplepos[0]), sortTP(tuplepos[1]))
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}

	dt, err := emp.read(empAllCol, [][]int{tuplepos[0], tuplepos[0], tuplepos[0], tuplepos[0], tuplepos[0], tuplepos[0], tuplepos[0], tuplepos[0]})
	if err != nil {
		t.Fatal(err)
	}  	
 	e, g = "[7839] [KING] [PRESIDENT] [0] [1981-11-17 00:00:00 +0000 UTC] [5000] [0] [10]", fmt.Sprintf("%v %s %s %v %v %v %v %v", dt[0], dt[1], dt[2], dt[3], dt[4], dt[5], dt[6], dt[7])
 	if e != g {
 		t.Errorf("expected: %v (type %T) and got: %v (type %T)", e, e, g, g)
 	}
}

func ps(s ...string) [][]byte {
	r := make([][]byte, len(s))
	for i := 0; i < len(s); i++ {
		r[i] = []byte(s[i])
	}
	return r
}
func pi(i ...int64) []int64 {
	return i
}
func pf(f ...float64) []float64 {
	return f
}
func pt(s ...string) []time.Time {
	r := make([]time.Time, len(s))
	for i := 0; i < len(s); i++ {
		r[i], _ = time.Parse("02-Jan-2006", s[i])
	}
	return r
}

/*
CREATE TABLE EMP
(EMPNO NUMERIC(4) NOT NULL,
ENAME VARCHAR(10),
JOB VARCHAR(9),
MGR NUMERIC(4),
HIREDATE DATETIME,
SAL NUMERIC(7, 2),
COMM NUMERIC(7, 2),
DEPTNO NUMERIC(2))

INSERT INTO EMP VALUES
(7369, 'SMITH', 'CLERK', 7902, '17-DEC-1980', 800, NULL, 20)
INSERT INTO EMP VALUES
(7499, 'ALLEN', 'SALESMAN', 7698, '20-FEB-1981', 1600, 300, 30)
INSERT INTO EMP VALUES
(7521, 'WARD', 'SALESMAN', 7698, '22-FEB-1981', 1250, 500, 30)
INSERT INTO EMP VALUES
(7566, 'JONES', 'MANAGER', 7839, '2-APR-1981', 2975, NULL, 20)
INSERT INTO EMP VALUES
(7654, 'MARTIN', 'SALESMAN', 7698, '28-SEP-1981', 1250, 1400, 30)
INSERT INTO EMP VALUES
(7698, 'BLAKE', 'MANAGER', 7839, '1-MAY-1981', 2850, NULL, 30)
INSERT INTO EMP VALUES
(7782, 'CLARK', 'MANAGER', 7839, '9-JUN-1981', 2450, NULL, 10)
INSERT INTO EMP VALUES
(7788, 'SCOTT', 'ANALYST', 7566, '09-DEC-1982', 3000, NULL, 20)
INSERT INTO EMP VALUES
(7839, 'KING', 'PRESIDENT', NULL, '17-NOV-1981', 5000, NULL, 10)
INSERT INTO EMP VALUES
(7844, 'TURNER', 'SALESMAN', 7698, '8-SEP-1981', 1500, 0, 30)
INSERT INTO EMP VALUES
(7876, 'ADAMS', 'CLERK', 7788, '12-JAN-1983', 1100, NULL, 20)
INSERT INTO EMP VALUES
(7900, 'JAMES', 'CLERK', 7698, '3-DEC-1981', 950, NULL, 30)
INSERT INTO EMP VALUES
(7902, 'FORD', 'ANALYST', 7566, '3-DEC-1981', 3000, NULL, 20)
INSERT INTO EMP VALUES
(7934, 'MILLER', 'CLERK', 7782, '23-JAN-1982', 1300, NULL, 10)

CREATE TABLE DEPT
(DEPTNO NUMERIC(2),
DNAME VARCHAR(14),
LOC VARCHAR(13) )

INSERT INTO DEPT VALUES (10, 'ACCOUNTING', 'NEW YORK')
INSERT INTO DEPT VALUES (20, 'RESEARCH', 'DALLAS')
INSERT INTO DEPT VALUES (30, 'SALES', 'CHICAGO')
INSERT INTO DEPT VALUES (40, 'OPERATIONS', 'BOSTON')

CREATE TABLE BONUS
(ENAME VARCHAR(10),
JOB VARCHAR(9),
SAL NUMERIC,
COMM NUMERIC)

CREATE TABLE SALGRADE
(GRADE NUMERIC,
LOSAL NUMERIC,
HISAL NUMERIC)

INSERT INTO SALGRADE VALUES (1, 700, 1200)
INSERT INTO SALGRADE VALUES (2, 1201, 1400)
INSERT INTO SALGRADE VALUES (3, 1401, 2000)
INSERT INTO SALGRADE VALUES (4, 2001, 3000)
INSERT INTO SALGRADE VALUES (5, 3001, 9999)
*/