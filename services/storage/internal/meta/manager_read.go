package meta

import (
	"context"
	"errors"
)

// Read 暂存实现，留待 STARSRC-493 完整实现。
func (m *manager) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	return nil, errors.New("meta: Read not implemented yet")
}
