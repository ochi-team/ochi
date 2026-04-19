// 100 GB
// TODO: move it to the config
const maxTableSize = 100 * 1024 * 1024 * 1024;

// TODO: pass amount of reserved memory from the caller for the ongoing merges,
// perform reservation once again on merging, after filtering
// FIXME: define a failure path when there is no enough space on the disk
pub fn getMaxTableSize(freeDiskSpace: u64) u64 {
    return @min(freeDiskSpace, maxTableSize);
}
