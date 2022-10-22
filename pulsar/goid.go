//go:build amd64 || arm64 || arm || 386 || mipsle
// +build amd64 arm64 arm 386 mipsle

package pulsar

func goid() int

// Goid returns the current goroutine id.
// It exactly matches goroutine id of the stack trace.
func Goid() int64 {
	return int64(goid())
}
