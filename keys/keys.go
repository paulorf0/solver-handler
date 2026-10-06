package keys

// Key identifies a bucket: Name is free, set by whoever uses the template.
type Key struct {
	Name   string
	Client string
}

func (k Key) String() string { return k.Name + "|" + k.Client }
