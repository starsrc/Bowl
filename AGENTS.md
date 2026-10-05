1. ISO Storage 项目会将 JuiceFS 作为一个包导入，并复用 JuiceFS 的各种代码。
   - 可以简单的将 #ISO-Storage 理解为一个有状态的云端版本的 JuiceFS。
2. ISO Storage V1 计划支持一写多读
3. ISO Storage V1 的大致工作原理是将服务端磁盘作为缓存区，最终的文件落盘在 B2 对象存储
   - 无缓存时: B2 -> ISO Storage Server -> ISO Storage Client
   - 存在缓存时: ISO Storage Server(NVME SDD) -> ISO Storage Client
   - ISO Storage V1 应当有管理热冷缓存的能力
4. ISO Storage V1 的 Client 端都在同机器内（比如 k8s 的 node 内的 pod，或者宿主机内的 KVM 虚拟机）
5. references/go-fuse 为参考代码，参考 fuse 协议的一些需求来定义合理的 rpc api 端点以完美实现