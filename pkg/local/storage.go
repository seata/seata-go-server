package local

// Storage  local data storage
type Storage interface {
	// Get returns the key value
	Get(key []byte) ([]byte, error)
	// Set sets the key value to the local storage
	Set(key, value []byte) error
	// Remove remove the key from the local storage
	Remove(key []byte) error
	// Range visit all values that start with prefix, set limit to 0 for no limit
	Range(prefix []byte, limit uint64, fn func(key, value []byte) bool) error
}
