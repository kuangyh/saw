# Saw

## Program Model

### Datum

Datum is a key-value pair, where key must by a string, value can by anything.

### Saw

Saw is a state machine with two interface:

- `Emit(Datum) error`: receives data points, modify its internal state.
- `Result() v error`: finish processing, returns computation result based on current status.

Saws can talk to other saws via. interfaces above.

As long as it keeps this model, a (conceptual) Saw can be run in parallel, even on multiple machines, concurrently receives data etc. But for most of Saws defined by user, they preserve state in memory, assumes serial call to Emit() and Result(), so good old day.

### Table

Table is a Saw that manages a map of Saws with same type, when data = (key, value) arrives, it's routed to the Saw dedicated for this key.

We provide serveral Table framework implementation taking care of parallel / distributed processing, result storage etc. User often just need to concern about Saws that handles a single key.

Table are often backed by persistent storage, that means we can store of calculation result in a key-value style storage for later use. A TableDriver is such a Saw that it reads Table result previously persistend, pass them to saws to perform next round of computation.

### Aggregators

Aggregators are just Saws that does aggregate computation, the simplest ones are like Sum, that add up all numbers it receives via. `Emit()` and return it when `Result()` called, more sophisicated examples are like quantitle, that can be (approx) calculated on-the-fly with constant space complexity ([ref](http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf)).

Combined with Aggregators and Tables, we're able to do stats at scale.

### Computation

Saws calling each other, end up having proccessed data store in some tables.

## Why?

Log processing, or [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) if we care about buzzwords. When run at scale, challenges hasn't been well studied and addressed aries.

Lots of progress had been made on data analysis tools --- including MR tools lately. But log processing is NOT data analysis despite similarity on the surface.

Log processing is:

- Ingest dirty, heterogeneous data from various sources.
- Convert, aggregate them into

## Scalability


## Fault tolerence
