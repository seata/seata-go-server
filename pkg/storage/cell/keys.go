package cell

import (
	"fmt"
)

func gCellKey(fid uint64) string {
	return fmt.Sprintf("__seata_%d_gid__", fid)
}

func manualCellKey(fid uint64) string {
	return fmt.Sprintf("__seata_%d_manual__", fid)
}
