# database

A hands-on database engineering repository for learning and production-style practice.

It includes:

- `waterfall/`: low-level storage and foundational components
- `newdb/`: database engine (`engine`) + command layer (`cli`) + tests + Rust GUI

## Why It Is Useful

- **End-to-end path**: from page storage and recovery to transaction visibility and GUI operations
- **Engineering loop**: regression tests, CI workflows, benchmark scripts, and runtime quality gates
- **Readable docs**: module-by-module architecture and implementation reviews

## Quick Links

- Source code: `newdb/`
- GUI app: `newdb/rust_gui/`
- Deep-dive PDF: `newdb/intro/out/newdb-intro.pdf`
- Developer guide: `docs/dev-guide.md`

## Refactor-Era Capabilities

- Recovery pipeline: analysis / redo / undo
- Rollback semantics: savepoint-level undo/redo + transaction rollback
- Point-in-time recovery: by LSN or timestamp (PITR)
- Observability: crash matrix, runtime report, nightly trend dashboard

## Documentation

- Intro/doc project: `newdb/intro/`
- Build and development guide: `docs/dev-guide.md`

## Repository

- GitHub: [skyline019/database](https://github.com/skyline019/database)
