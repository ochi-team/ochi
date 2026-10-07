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
        fn swap(self: *const Self, i: usize, j: usize) void {
            const tmp = self.array.items[i];
            self.array.items[i] = self.array.items[j];
            self.array.items[j] = tmp;
        }

        /// Establish heap invariants from an unsorted array.
        /// Complexity: O(n) where n = len()
        pub fn heapify(self: *const Self) void {
            const n = self.len();
            if (n <= 1) return;

            var i: isize = @as(isize, @intCast(n / 2)) - 1;
            while (i >= 0) : (i -= 1) {
                _ = self.down(@intCast(i), n);
            }
        }

        /// Push an element onto the heap.
        /// Complexity: O(log n) where n = len()
        pub fn push(self: *const Self, x: T) !void {
            try self.array.append(self.allocator, x);
            self.up(self.len() - 1);
        }

        /// Remove and return the minimum element from the heap.
        /// Asserts that the heap is not empty.
        /// Complexity: O(log n) where n = len()
        pub fn pop(self: *const Self) T {
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
        pub fn remove(self: *const Self, i: usize) T {
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
        pub fn fix(self: *const Self, i: usize) void {
            if (!self.down(i, self.len())) {
                self.up(i);
            }
        }

        /// Move element at index j up to its proper position
        fn up(self: *const Self, j_start: usize) void {
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
        fn down(self: *const Self, start_idx: usize, n: usize) bool {
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
