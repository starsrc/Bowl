# Delegate POSIX Locks and Extended Attributes to JuiceFS Core Engine

POSIX byte-range locks, BSD file locks, and extended attributes involve intricate concurrency edge cases and file system format specifications. We decided to delegate `GetLk`, `SetLk`, `SetLkw`, `Flock`, `GetXAttr`, `SetXAttr`, `ListXAttr`, and `RemoveXAttr` directly to the underlying JuiceFS metadata and lock managers. This prevents duplicated locking algorithms and ensures compliance with kernel FUSE semantics.
