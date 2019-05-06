package id

// Generator id generator
type Generator interface {
	Gen() (uint64, error)
}

// NewMemGenerator returns a mem generator
func NewMemGenerator() Generator {
	return &memGenerator{}
}
