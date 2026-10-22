package disk

// 导出内部辅助函数供测试包 disk_test 使用。
var (
	ExportParseDiskID  = parseDiskID
	ExportToDomainDisk = toDomainDisk
)
