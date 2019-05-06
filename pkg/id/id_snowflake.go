package id

import (
	"time"

	"github.com/sony/sonyflake"
)

type snowflakeGenerator struct {
	gen *sonyflake.Sonyflake
}

// NewSnowflakeGenerator returns a id generator implemention by snowflake
func NewSnowflakeGenerator(machineID uint16) Generator {
	return &snowflakeGenerator{
		gen: sonyflake.NewSonyflake(sonyflake.Settings{
			StartTime: time.Now(),
			MachineID: func() (uint16, error) {
				return machineID, nil
			},
		}),
	}
}

func (g *snowflakeGenerator) Gen() (uint64, error) {
	return g.gen.NextID()
}
