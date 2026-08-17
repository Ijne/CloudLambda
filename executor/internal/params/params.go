package params

import (
	"lambda/internal/vars"
	"time"
)

type Params struct {
	EnvType    vars.EnvType
	CodeSource string
	RootFS     string

	Flags uintptr

	MemoryLimit int64
	CPUQuota    int64
	TimeLimit   time.Duration
}
