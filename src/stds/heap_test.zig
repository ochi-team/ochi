// Min-heap implementation based on Go's container/heap
// A heap is a tree with the property that each node is the
// minimum-valued node in its subtree.
//
// The minimum element in the tree is the root, at index 0.

const std = @import("std");
const ArrayList = std.ArrayList;

const Heap = @import("heap.zig").Heap;
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
