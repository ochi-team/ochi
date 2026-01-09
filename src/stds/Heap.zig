// Min-heap implementation based on Go's container/heap
// A heap is a tree with the property that each node is the
// minimum-valued node in its subtree.
//
// The minimum element in the tree is the root, at index 0.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

/// Generic min-heap implementation.
/// T: The type of elements stored in the heap
/// lessFn: Comparison function - returns true if a < b
pub fn Heap(comptime T: type, comptime lessFn: fn (a: T, b: T) bool) type {
    return struct {
        array: *ArrayList(T),
        allocator: Allocator,

        const Self = @This();

        /// Initialize a heap with a pointer to an existing ArrayList
        pub fn init(allocator: Allocator, items: *ArrayList(T)) Self {
            return Self{
                .array = items,
                .allocator = allocator,
            };
        }

        /// Returns the number of elements in the heap
        pub fn len(self: *const Self) usize {
            return self.array.items.len;
        }

        /// Swap elements at indices i and j
        fn swap(self: *Self, i: usize, j: usize) void {
            const tmp = self.array.items[i];
            self.array.items[i] = self.array.items[j];
            self.array.items[j] = tmp;
        }

        /// Establish heap invariants from an unsorted array.
        /// Complexity: O(n) where n = len()
        pub fn heapify(self: *Self) void {
            const n = self.len();
            if (n <= 1) return;

            var i: isize = @as(isize, @intCast(n / 2)) - 1;
            while (i >= 0) : (i -= 1) {
                _ = self.down(@intCast(i), n);
            }
        }

        /// Push an element onto the heap.
        /// Complexity: O(log n) where n = len()
        pub fn push(self: *Self, x: T) !void {
            try self.array.append(self.allocator, x);
            self.up(self.len() - 1);
        }

        /// Remove and return the minimum element from the heap.
        /// Asserts that the heap is not empty.
        /// Complexity: O(log n) where n = len()
        pub fn pop(self: *Self) T {
            const n = self.len();
            std.debug.assert(n > 0);

            self.swap(0, n - 1);
            _ = self.down(0, n - 1);
            return self.array.pop().?;
        }

        /// Peek at the minimum element without removing it.
        /// Returns null if the heap is empty.
        pub fn peek(self: *const Self) ?T {
            if (self.len() == 0) return null;
            return self.array.items[0];
        }

        /// Remove and return the element at index i from the heap.
        /// Complexity: O(log n) where n = len()
        pub fn remove(self: *Self, i: usize) T {
            const n = self.len();
            std.debug.assert(i < n);

            const last_idx = n - 1;
            if (last_idx != i) {
                self.swap(i, last_idx);
                if (!self.down(i, last_idx)) {
                    self.up(i);
                }
            }
            return self.array.pop().?;
        }

        /// Re-establish heap ordering after the element at index i has changed.
        /// Complexity: O(log n) where n = len()
        pub fn fix(self: *Self, i: usize) void {
            if (!self.down(i, self.len())) {
                self.up(i);
            }
        }

        /// Move element at index j up to its proper position
        fn up(self: *Self, j_start: usize) void {
            var j = j_start;
            while (true) {
                if (j == 0) break;
                const i = (j - 1) / 2; // parent
                if (!lessFn(self.array.items[j], self.array.items[i])) {
                    break;
                }
                self.swap(i, j);
                j = i;
            }
        }

        /// Move element at index start_idx down to its proper position
        /// Returns true if the element was moved
        fn down(self: *Self, start_idx: usize, n: usize) bool {
            var i = start_idx;
            while (true) {
                const j1 = 2 * i + 1;
                if (j1 >= n) break; // j1 >= n means i is a leaf node

                var j = j1; // left child
                const j2 = j1 + 1; // right child
                if (j2 < n and lessFn(self.array.items[j2], self.array.items[j1])) {
                    j = j2; // right child is smaller
                }

                if (!lessFn(self.array.items[j], self.array.items[i])) {
                    break;
                }

                self.swap(i, j);
                i = j;
            }
            return i > start_idx;
        }

        /// Get the element at the second position (useful for k-way merge)
        /// Returns null if heap has less than 2 elements
        pub fn peekNext(self: *const Self) ?T {
            const n = self.len();
            if (n < 2) return null;
            if (n < 3) return self.array.items[1];

            const a = self.array.items[1];
            const b = self.array.items[2];
            return if (lessFn(a, b)) a else b;
        }
    };
}

// Tests
const testing = std.testing;

fn lessInt(a: i32, b: i32) bool {
    return a < b;
}

fn greaterInt(a: i32, b: i32) bool {
    return a > b;
}

test "Heap: init and deinit" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    try testing.expectEqual(@as(usize, 0), heap.len());
}

test "Heap: push and pop" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    try heap.push(10);
    try heap.push(5);
    try heap.push(20);
    try heap.push(1);

    try testing.expectEqual(@as(usize, 4), heap.len());
    try testing.expectEqual(@as(i32, 1), heap.peek().?);

    try testing.expectEqual(@as(i32, 1), heap.pop());
    try testing.expectEqual(@as(i32, 5), heap.pop());
    try testing.expectEqual(@as(i32, 10), heap.pop());
    try testing.expectEqual(@as(i32, 20), heap.pop());

    try testing.expectEqual(@as(usize, 0), heap.len());
}

test "Heap: heapify" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    // Add elements without maintaining heap property
    try heap.array.append(heap.allocator, 20);
    try heap.array.append(heap.allocator, 19);
    try heap.array.append(heap.allocator, 18);
    try heap.array.append(heap.allocator, 17);
    try heap.array.append(heap.allocator, 16);

    // Now establish heap property
    heap.heapify();

    // Elements should come out in sorted order
    var prev = heap.pop();
    while (heap.len() > 0) {
        const curr = heap.pop();
        try testing.expect(prev <= curr);
        prev = curr;
    }
}

test "Heap: all same elements" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    var i: usize = 0;
    while (i < 20) : (i += 1) {
        try heap.push(0);
    }

    heap.heapify();

    while (heap.len() > 0) {
        const x = heap.pop();
        try testing.expectEqual(@as(i32, 0), x);
    }
}

test "Heap: sorted insertion" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    var i: i32 = 20;
    while (i > 0) : (i -= 1) {
        try heap.push(i);
    }

    var expected: i32 = 1;
    while (heap.len() > 0) {
        const x = heap.pop();
        try testing.expectEqual(expected, x);
        expected += 1;
    }
}

test "Heap: remove" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    var i: i32 = 0;
    while (i < 10) : (i += 1) {
        try heap.push(i);
    }

    // Remove from the end
    var expected: i32 = 9;
    while (heap.len() > 0) {
        const idx = heap.len() - 1;
        const x = heap.remove(idx);
        try testing.expectEqual(expected, x);
        expected -= 1;
    }
}

test "Heap: remove from front" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    var i: i32 = 0;
    while (i < 10) : (i += 1) {
        try heap.push(i);
    }

    // Remove from index 0 (same as pop)
    var expected: i32 = 0;
    while (heap.len() > 0) {
        const x = heap.remove(0);
        try testing.expectEqual(expected, x);
        expected += 1;
    }
}

test "Heap: fix" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    var i: i32 = 200;
    while (i > 0) : (i -= 10) {
        try heap.push(i);
    }

    try testing.expectEqual(@as(i32, 10), heap.array.items[0]);

    // Change the root element
    heap.array.items[0] = 210;
    heap.fix(0);

    // Verify heap property is maintained
    var prev = heap.pop();
    while (heap.len() > 0) {
        const curr = heap.pop();
        try testing.expect(prev <= curr);
        prev = curr;
    }
}

test "Heap: peekNext" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, lessInt).init(testing.allocator, &list);

    try testing.expectEqual(@as(?i32, null), heap.peekNext());

    try heap.push(10);
    try testing.expectEqual(@as(?i32, null), heap.peekNext());

    try heap.push(5);
    try testing.expectEqual(@as(i32, 10), heap.peekNext().?);

    try heap.push(3);
    try heap.push(15);
    // Heap is now [3, 5, 10, 15] or similar
    // peekNext should return the smaller of positions 1 and 2
    const next = heap.peekNext().?;
    try testing.expect(next == 5 or next == 10);
}

test "Heap: max heap" {
    var list = ArrayList(i32).empty;
    defer list.deinit(testing.allocator);

    var heap = Heap(i32, greaterInt).init(testing.allocator, &list);

    try heap.push(10);
    try heap.push(5);
    try heap.push(20);
    try heap.push(1);

    // With greaterInt, we get a max heap
    try testing.expectEqual(@as(i32, 20), heap.pop());
    try testing.expectEqual(@as(i32, 10), heap.pop());
    try testing.expectEqual(@as(i32, 5), heap.pop());
    try testing.expectEqual(@as(i32, 1), heap.pop());
}
