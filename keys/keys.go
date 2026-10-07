package keys

import (
	"fmt"
	"strings"
)

// Key identifies a bucket: Name is free, set by whoever uses the template.
type Key struct {
	Name   string
	Client string
}

func (k Key) String() string { return k.Name + "|" + k.Client }

// Parse reads "name|client". It splits on the last "|", since Name is free and may contain it.
func Parse(s string) (Key, error) {
	i := strings.LastIndex(s, "|")
	if i < 0 {
		return Key{}, fmt.Errorf("chave %q fora do padrão chave|cliente", s)
	}
	return Key{Name: s[:i], Client: s[i+1:]}, nil
}
