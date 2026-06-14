// TODO: pass amount of reserved memory from the caller for the ongoing merges,
// perform reservation once again on merging, after filtering
// TODO: define a failure path when there is no enough space on the disk
// When there is no enough space on the disk we must remove the oldest partition
// and continue the flushing process
// For that matter configuration options must be introduced:
// maxTableSize - max space amount a table may take, default is 100GB,
// log error if the configured option is larger than 5-10% of the total disk space
const maxTableSize = @import("../../Conf.zig").maxTableSize;
pub fn getMaxTableSize(freeDiskSpace: u64) u64 {
    return @min(freeDiskSpace, maxTableSize);
}
