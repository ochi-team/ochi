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

pub fn sleepOrStop(self: *Stop, io: Io, ns: u64) void {
    if (self.isStopped()) return;

    self.event.waitTimeout(io, .{ .duration = .{
        .clock = .real,
        .raw = .fromNanoseconds(ns),
    } }) catch |err| switch (err) {
        error.Timeout => {},
        error.Canceled => {},
    };
}
