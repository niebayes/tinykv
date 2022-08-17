package storage

// Modify is a single modification to TinyKV's underlying storage.
type Modify struct {
	// empty interface.
	// @ref: https://stackoverflow.com/questions/23148812/whats-the-meaning-of-interface
	Data interface{} // the type of the modification: Put/Delete
}

// set key Cf_Key's value as Value in the underlying store.
type Put struct {
	// []byte vs. string in Go.
	// @ref: https://syslog.ravelin.com/byte-vs-string-in-go-d645b67ca7f
	Key   []byte // key is stored as a byte slice.
	Value []byte // values is storted as a byte slice.
	Cf    string // which column family to put the key-value pair.
}

// delete key Cf_Key from the underlying store.
type Delete struct {
	Key []byte
	Cf  string
}

// retrieve the Key field from a Modify instance.
func (m *Modify) Key() []byte {
	// type switches.
	// @ref: https://go.dev/tour/methods/16
	switch m.Data.(type) {
	case Put:
		// cast interface into struct before using it.
		// @ref: https://stackoverflow.com/questions/50939497/golang-cast-interface-to-struct
		return m.Data.(Put).Key
	case Delete:
		return m.Data.(Delete).Key
	}

	// nil in Go: nil pointer, nil slice, nil interface, nil map, nil channel, nil function.
	// @ref: https://www.jianshu.com/p/dd80f6be7969
	return nil
}

// retrieve the Value field from a Modify instance.
func (m *Modify) Value() []byte {
	// type assertions.
	// @ref: https://go.dev/tour/methods/15
	if putData, ok := m.Data.(Put); ok {
		return putData.Value
	}

	return nil
}

// retrieve the Cf field from a Modify instance.
func (m *Modify) Cf() string {
	switch m.Data.(type) {
	case Put:
		return m.Data.(Put).Cf
	case Delete:
		return m.Data.(Delete).Cf
	}

	// default values for types in Go.
	// @ref: https://yourbasic.org/golang/default-zero-value/
	return ""
}
