package esbulk

import "strings"

// ArrayFlags allows to store lists of flag values.
type ArrayFlags []string

func (f *ArrayFlags) String() string {
	return strings.Join(*f, ", ")
}

// Set appends a value.
func (f *ArrayFlags) Set(value string) error {
	*f = append(*f, value)
	return nil
}
