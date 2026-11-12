const std = @import("std");
const Io = std.Io;

const Stop = @This();

event: Io.Event = .unset,

pub fn stop(self: *Stop, io: Io) void {
    self.event.set(io);
}

pub fn isStopped(self: *const Stop) bool {
    return self.event.isSet();
}
