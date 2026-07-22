pub fn swapRemove(T: type, items: []T, i: usize) []T {
    var rest = items;
    rest[i] = rest[rest.len - 1];
    rest[rest.len - 1] = undefined;
    rest.len -= 1;
    return rest;
}
