# Agent Guidelines for Seqor

## Build/Test Commands
- **Build**: `zig build`
- **Run**: `zig build run`
- **Test all**: `zig build test`
- **Test single**: `zig build test -Dtest-filter="SIGTERM"` (use substring from test name)

## Code Style
- **Language**: Zig, follows standard Zig conventions
- **Imports**: Group by std library, external deps (datetime, httpz, snappy), then local modules
- **Types**: Use explicit types (u8, u16, u32, i64, i128, etc.), struct fields use camelCase
- **Functions**: Public functions use `pub fn`, private without `pub`, use snake_case
- **Naming**: camelCase for variables/fields, PascalCase for types/structs, snake_case for functions
- **Error handling**: Use `try` for error propagation, explicit error types where needed
- **Comments**: TODO comments for future work, doc comments (///) for public API
- **Memory**: Use passed allocator, defer cleanup with deinit/destroy/free
- **Testing**: Tests in separate `*_test.zig` files, imported via test block in main module

## Project Context
- Loki-compatible log database written in Zig
- HTTP server using httpz library
- Main entry: `src/main.zig`, server logic: `src/server.zig`
- Signal handling for graceful shutdown (SIGTERM)
