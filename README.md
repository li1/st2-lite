# st2 lite

A minimal implementation of SnailTrail 2.
Constructs a fixed-window PAG and works with Timely / DD out of the box.
The PAG can be visualized using the `dashboard`, a cljs+D3.js frontend.

st2 lite is not meant to be an end-to-end online analysis solution. For that, try out [SnailTrail 2](https://github.com/li1/snailtrail).

## Running

Any Timely / DD computation can be profiled online out of the box:

1. Run the source computation with env var `TIMELY_WORKER_LOG_ADDR="127.0.0.1:1234"`
2. Run st2 with `#source computation workers` as command-line argument to construct the PAG (cf. `src/main.rs`).

To run offline:

1. Modify the source computation to write log events to disk (cf. `examples/minimal.rs`).
2. Run st2 with `f` as command-line argument from the same directory.

## Repository Structure

- Shared structs: `src/lib.rs`
- PAG construction operators: `src/pag.rs`
- Exemplary PAG construction / profiling: `src/main.rs`
- Source computation examples: `examples`
- Interactive PAG visualization: `dashboard`

## Comparison with [SnailTrail 2](https://github.com/li1/snailtrail)

Pro:

- Works out of the box with any Timely / Differential computation
- Small codebase, easy to extend / modify
- For Clojure users: dashboard easy to understand, interactive development

Con:

- Slightly slower since source computation doesn't use adapter library
- Fixed window pag instead of richer epoch-based PAG semantics
- Dashboard only visualizes PAG, doesn't contain further analyses
- No CLI
- **@TODO**: Dashboard isn't connected to `st2` backend. Currently, a PAG has to be written to data.cljs for analysis.
- **@TODO**: Dashboard's PAG time range filter doesn't work yet.
