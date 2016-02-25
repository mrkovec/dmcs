
package dmcs

import (
	"fmt"
)
//Binary prefix
type byteSize float64
const (
	_           = iota // ignore first value by assigning to blank identifier
	KB byteSize = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
	ZB
	YB
)

func (b byteSize) String() string {
	switch {
	case b >= YB:
		return fmt.Sprintf("%.2fYiB", b/YB)
	case b >= ZB:
		return fmt.Sprintf("%.2fZiB", b/ZB)
	case b >= EB:
		return fmt.Sprintf("%.2fEiB", b/EB)
	case b >= PB:
		return fmt.Sprintf("%.2fPiB", b/PB)
	case b >= TB:
		return fmt.Sprintf("%.2fTiB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2fGiB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2fMiB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.0fKiB", b/KB)
	}
	return fmt.Sprintf("%dB", int(b))
}

//SI prefixes
type statNumber float64
const (
	KILO statNumber = 1e3
	MEGA statNumber = 1e6
	GIGA statNumber = 1e9
	TERA statNumber = 1e12
	PETA statNumber = 1e15
	EXA  statNumber = 1e18
)

func (b statNumber) String() string {
	switch {
	case b >= EXA:
		return fmt.Sprintf("%.2fE", b/EXA)
	case b >= PETA:
		return fmt.Sprintf("%.2fP", b/PETA)
	case b >= TERA:
		return fmt.Sprintf("%.2fT", b/TERA)
	case b >= GIGA:
		return fmt.Sprintf("%.2fG", b/GIGA)
	case b >= MEGA:
		return fmt.Sprintf("%.2fM", b/MEGA)
	case b >= KILO:
		return fmt.Sprintf("%.2fk", b/KILO)
	}
	return fmt.Sprintf("%d", int(b))
}
