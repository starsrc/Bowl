# Use Local Directory Object Storage Driver for Chunks

Initial deployment and automated testing require fast, zero-dependency block storage for file slices. We decided to configure JuiceFS's underlying object storage driver using the local file directory scheme (`file:///var/lib/bowl/data/<disk_id>`). This provides sub-millisecond local NVMe performance without requiring third-party storage daemons, while maintaining full configuration compatibility with remote S3 and Backblaze B2 targets.
