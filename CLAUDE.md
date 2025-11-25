# Agent Guidelines for Seqor

## Build/Test Commands
- **Build**: `zig build`
- **Run**: `zig build run`
- **Test all**: `zig build test`
- **Test single**: `zig build test -Dtest-filter="SIGTERM"` (use substring from test name)

## Code Style
- **Language**: Zig, follows standard Zig conventions
- **Imports**: Group by std library, external deps (datetime, httpz, snappy), then local modules
- **Naming**: PascalCase for types/structs, camelCase for variables/fields and functions, but if a function returns a type - PascalCase
- **Error handling**: Use `try` for error propagation, explicit error types where needed
- **Comments**: TODO comments for future work, doc comments (///) for public API
- **Memory**: Use passed allocator, defer cleanup with deinit/destroy/free

### docs discovery
Use zigdoc to validate the API of the used Zig version, e.g.:
- zigdoc std.ArrayList
- zigdoc std.mem.Allocator
- zigdoc std.http.Server

## Project Context
- Loki-compatible log database written in Zig
- HTTP server using httpz library
- Main entry: `src/main.zig`, server logic: `src/server.zig`
- Signal handling for graceful shutdown (SIGTERM)


