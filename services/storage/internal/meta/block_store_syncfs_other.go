//go:build !linux

package meta

func syncFSDataDir(_ string) (bool, error) {
	return false, nil
}
