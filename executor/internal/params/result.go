package params

type Result struct {
	ExitCode int
	TimedOut bool
	Stdout   []byte
}
