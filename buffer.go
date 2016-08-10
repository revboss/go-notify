package notify

import (
	"io"
)

type Buffer []byte

func (b *Buffer) Read(p []byte) (int, error) {
	copy(p[0:], []byte(*b)[0:len(*b)])

	if len([]byte(*b)) > len(p) {
		return len(p), io.EOF
	} else {
		return len([]byte(*b)), io.EOF
	}

	return 0, nil
}

func (b *Buffer) Write(p []byte) (int, error) {
	*b = append(*b, p...)
	return 0, nil
}
