# 0001. FUSE Lookup 最小元数据结构与接口契约设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务需要通过 RPC 对接 Linux FUSE 内核的 `lookup` 接口。POSIX FUSE 的 `lookup(parent, name)` 调用强制要求返回对应 `fuse_entry_param` 规范的元数据（包含 Inode 编号、代数 Generation、POSIX 物理属性集合以及内核缓存超时控制）。为了在满足当前单项查找诉求的同时，避免过度设计并保证契约在后续接口中的复用性，需要确定最小化数据结构与契约规范。

## 决策
1. **目录与 Package 规范**：
   - 文件统一置于 `proto/storage/v1/storage.proto`，声明 Package 为 `storage.v1`，严格对齐 Buf 目录命名空间规范。
2. **元数据模型解耦**：
   - 抽象实体 `Inode` 承载节点唯一持久化标识 `ino`、代数 `generation` 及关联属性 `attr`。
   - 抽象物理属性 `InodeAttribute` 封装 POSIX `struct stat` 属性（`mode`, `size`, `nlink`, `uid`, `gid`, `rdev`, `blksize`, `blocks`, `atime`, `mtime`, `ctime`），便于未来在 `GetAttr`、`SetAttr`、`ReadDir` 等操作中直接复用。
3. **Lookup 接口设计**：
   - 请求体 `LookupRequest` 仅包含 `parent_ino` 与 `name`，通过 `buf.validate` 严格校验输入合法性（禁止空值、超长及包含路径分隔符 `/` 或空字节 `\x00`）。
   - 响应体 `LookupResponse` 包含命中的 `Inode` 及内核缓存控制 `entry_timeout_seconds` 与 `attr_timeout_seconds`。
4. **错误语义规范**：
   - 目标文件或父节点不存在时，统一返回 gRPC 标准状态码 `NotFound`（客户端映射至 POSIX `-ENOENT`），不使用报文内布尔标志位。
   - 父节点存在但其 `mode` 非目录（`(mode & S_IFMT) != S_IFDIR`）时，返回 gRPC 标准状态码 `FailedPrecondition`（客户端映射至 POSIX `-ENOTDIR`）。
5. **时间表示**：
   - 时间字段全部采用官方 `google.protobuf.Timestamp`，支持纳秒精度。

## 结果
- **积极影响**：实体标识与物理属性分离，结构高内聚且易于复用；RPC 入口具备强类型边界校验；错误契约符合微服务最佳实践。
- **代价考量**：Protobuf 结构增加了一层嵌套，但规避了后续接口扩展时的字段冗余。
