# rusty-mcrouter docs
rusty-mcrouter is based off meta's mcrouter, but not a strict clone. we follow the following philosophy:
- general compatibility in terms of features - most routes available in mcrouter should eventually be available in `rusty-mcrouter`
- general compatibility in terms of usage - a mcrouter config file should more or less work in rusty-mcrouter
- use OSS alternatives to meta internal products where possible - instead of fb's `artillery`, we use prometheus for observability, etc.

## layout
- `architecture` - how rusty-mcrouter is built as of now
- `design` - numbered and dated plans for features that we build, that are usually based off a feature found in **real** mcrouter 
- `reference` - how **real** mcrouter behaves or is designed, one topic per document that is written while researching for a file in `design`
- `mvp` - original documentation that was written when we aimed for a strict clone of mcrouter. it is an archive and should no longer be edited

## where does a doc go?
- how rusty-mcrouter works right now ? -> `architecture/`
- a plan for something that will be built ? -> `design/NNNN-<topic>.md`
- how upstream mcrouter does something? -> `reference`
