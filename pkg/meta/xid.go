package meta

// FragmentXID fragment xid
type FragmentXID struct {
	GID        uint64 `json:"gid"`
	FragmentID uint64 `json:"fid"`
}

// NewFragmentXID returns fragment xid
func NewFragmentXID(gid, fid uint64) FragmentXID {
	return FragmentXID{
		GID:        gid,
		FragmentID: fid,
	}
}
