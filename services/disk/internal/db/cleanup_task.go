package db

// 磁盘物理清理任务（Disk Cleanup Task）生命周期状态常量。
const (
	// CleanupTaskStatusPending 表示任务已创建，等待清理工作器拉取调度。
	CleanupTaskStatusPending int16 = 1

	// CleanupTaskStatusRunning 表示清理工作器已认领任务，正在执行物理对象与本地切片清理。
	CleanupTaskStatusRunning int16 = 2

	// CleanupTaskStatusCompleted 表示物理对象与本地切片已全部清理完成。
	CleanupTaskStatusCompleted int16 = 3

	// CleanupTaskStatusFailed 表示清理流程执行失败。
	CleanupTaskStatusFailed int16 = 4
)
