package disk

import (
	"context"
)

// DeleteDisk 根据磁盘 ID 物理删除磁盘。
// 规则：
// 1. 静态参数校验：diskID 必须为有效非全零 UUID，校验失败返回 ErrInvalidArgument。
// 2. 调用持久层 DeleteDisk 执行删除：若发生底层错误则透明透传。
// 3. 若删除受影响行数为 0，说明目标磁盘不存在，返回 ErrNotFound。
// 4. 删除成功返回 nil。
func (s *service) DeleteDisk(ctx context.Context, diskID string) error {
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return err
	}

	rows, err := s.store.DeleteDisk(ctx, pgID)
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}
