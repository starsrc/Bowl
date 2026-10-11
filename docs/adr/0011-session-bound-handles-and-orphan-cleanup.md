# Bind File and Directory Handles to Mount Sessions for Orphan Recovery

Clients that crash or lose network connectivity leave open handles and dangling locks on the file system. We decided that `StorageService` strictly indexes all allocated file handles (`fh`) and directory handles (`dh`) under their respective `session_id`. When a session is unmounted or evicted following a lease timeout, the server automatically flushes dirty chunks, unlocks associated locks, and closes orphaned handles.
