# rusty-mcrouter docs

- `mcrouter/` — how meta's real mcrouter works. reference for the system we're modeling after.
- `architecture/` — how rusty-mcrouter is actually built right now
- `design/` — what we're planning to build and why, tied back to the relevant `mcrouter/` doc.

each folder's `overview.md` is the entry point.

## where does a new doc go?

- about real mcrouter, regardless of what we build? → `mcrouter/`
- how rusty works right now? → `architecture/`
- a plan for something we're gonna build? → `design/`

if a design doc maps to a mcrouter concept, reuse the same filename across folders so it's obvious which goes with which:

```
mcrouter/threading-model.md      how mcrouter does it
design/threading-model.md        what we're building + why
architecture/threading-model.md  how it ended up
```
