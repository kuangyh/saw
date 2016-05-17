# Saw

## Background: ETL

ETL == Extract, Transform and Load, or "log processing" in muggle world.

### Use case: session log

- Merge logs from clients and different servers, group by session-id
- Raw logs are dirty, do clean up filter, normalization --- tighly coupled to biz logic
- Assume 2h session window, provided aggregated detailed session data asap.

## Use case: dimensional metrics

- Use session data.
- Extract 10+ dimensions and 100+ event signals for each session
- Extraction logic can be complicated but limited to a single session.
- Populate to SQL DB (or Dremel / BigQuery) for interactive queries.

Typical query:

    SELECT sum/avg/quantile/...(signals)
    WHERE exp_id IN (...) AND dimension_x=..
    GROUP BY exp_id, dimension_y, dimension_z ...

### Use case: realtime spike detection

- Realtime hot topics, spam detection
- Directly streaming log data
- Find dimensions and signals, maintain an in-memory time series storage.

### What we have today

- Log ingestion tools
  - Heka, fluented etc.
  - Focus on convert / unfiy different log sources, configuration driven, coding not required in most cases.
  - Lack of aggregation support
- Data analysis platforms
  - New tools like Spark, Dataflow, focus on ease the task of constructing complex computation pipeline / fow with planner / compiler.
  - Functional programming model.
- Workflow management
  - DAG based MR pipeline management, Airflow etc.
- Time-series DB
  - Specific application (monitoring: prometheus)

### The gap

- Programming model for dirty data
  - Complexity is in processing single / small set (session) data, not overall computation model, facny distributed query planner doesn't help.
  - Complexity is in biz logic, not in converting one format to another, configurable generic modules doesn't help.
- Manageable computation model
  - Computation model of ETL pipelines tend to be simple, static.
  - But consider real-time streaming + small batch needs above, we want to do it in same framework.
  - +data dependencies and modules owned by different teams.
  - The pipeline must be predictable, easy monitor / debug / optimize, auto planner can harm.
- Pipeline Ops
  - Updating binary, changing protobuf.
  - Configurations shared between different modules / teams.
  - Monitoring, failure / overrun detection + data dependency management
  - Current tools not integrated well.

### Goal

- A programming framework for ETL job.
- Ops support as first class citizen, from framework to tools.
- Integrate with current tools, computation platforms, storages.
- CPU + IO efficiency

### Non-goal

- Another distributed computing framework.

## Saw, the programming model

### Main inspirations

- [Sawzall](http://static.googleusercontent.com/media/research.google.com/en//archive/sawzall-sciprog.pdf): table abstraction.
- [Millwheel](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/41378.pdf): generalized computation model.

### Saw

- A (possibly stateful) computation unit.
- `Emit(datum)`
  - Main interface, process a single input
  - Datum: a KV pair, where key is string and V can be anything (protobuf on wire)
  - Process data, send converted datums to any number of other saw, modify self state.
- `Result() -> Any`
  - Returns aggregated computation result based on all data received from `Emit()`
- Somewhat similar to Actor but
  - Message passing is synchronized.
  - Saw receives concurrent Emit unless otherwise noted.

### Table

- Is a framework container Saw.
- `table[sum]`: upon reciving datum `<kev, value>`, it group datum by key, and use aggregator `sum` to aggregate.
- sum is also a saw, for every datum emitted, it roughly do `self.curr += datum.value`
- Its `Result()` returns a map of `Result()` returned by aggregator for each key, and can optionally dump to persistent storage.
- Because the result is essentially a bunch of datum `<key, value>`, we can (in parallel) send its result to other saws, that's often called "re-saw"

Table is the main abstraction and power source of Saw, it takes care of:

- Data / computation parallelism and distribution
  - While table receives `Emit()` concurrently, it makes sure aggregator for each key gets data in sequence so normally aggregator implementor don't need to consider concurrency problem.
  - A distributed table is able to run aggregators in different nodes, using MR for example.
- Data persistentcy
  - As long as saw implements encoder / decoder, computation result (or even current state of aggregators) can be stored.

### Driver

- Gets data input from outside world.
  - Read input from local file / network storage, Kafka etc.
- Manage local runtime
  - Streaming or batch, parallelism.
- Detect data dependency and do remote excution
  - Provide tools to write complex workflow / pipeline

### Discussion

User-defined saws often falls in one of the two categories:

- Stateless "mapper"
- Stateful aggregator or "reducer"

Mapper only need to implement Emit. (`Result()` makes no sense to stateless component):

    func Emit(datum Datum) error {
      if !isValidData(datum) {
        return
      }
      userDimensions := extractDimensions(datum)
      metrics := normalzedMetrics(datum)
      // Store all data to session
      if isSessionEvent(datum) {
        sessionID := extractSessionID(datum)
        SessionTable.Emit(MakeDatum(sessionId, datum.Value))
      }
      // Event-specific aggregations
      if isQuery(datum) {
        for word := range strings.Split(data.Value.Query) {
          if isStopWord(word) {
            continue
          }
          QueryTable.Emit(makeQueryDatum(userDimensions, word, metrics))
        }
      } else isAdClick(datum) {
        ...
        adDimnesions := extractAdDimensions(datum)
        AdClickTable.Emit(makeAdClickDatum(Datum(userDimensions, adDimensions, metrics)))
      }
    }

Note the significant difference from MR framework: it sends data to different destination (represented as tables in Saw), with different key schema.

- In MR, the same semantic need be done using seperate mapper functions, resulting splitted code of closely related logics (dimension extraction and metric normalization etc.), and inefficient runtime.
- In MR, data flow is controlled by job configuration code that combines small piecies of mapper / reducer functions, in here, it's controlled within Emit(), well aligned with bussiness needs.

Comparing to Millwheel, one key differences is the absent of key extractor --- the data producer instead of consumer determine key schema. That sacrifice some flexibility but for good:

- Because of its static nature, consumer key extraction flexibility is not that valuable to ETL tasks.
- Data produced needed to be stored in specified structure to support common consumer needs,  key schema is part of this storage design --- same of computation distribution. You either get bad performance or sacrifice flexibility.
- If the flexibility is really needed, a simple in-memory, synchronized pubsub does the trick.

In writing reducer, a common pattern is to do mini-aggregation, by providing simple in-memory table (essentially memory map) and common standard aggregators, they can be done in a unified way.

TODO: code samples

## Implementation

### Early High-level decisions

- Go
  - It's not data analysis --- or I'd better use Python
  - Static type, code analysis tool, enable debugging and auditing at scale.
  - Acceptable performance at many-core architecture.
  - Easy deploy
- Optimize of single instance first
  - Single machine is powerful enough to cover 80% of needs today, Go uses multi-cores well.
  - They're very cheap on cloud --- 32 cores, 120G RAM preemptible instance costs < $0.5 / hr on [GCE](https://cloud.google.com/compute/pricing), and you can simply disable it when it's not actively used.
  - Familiar, flexible programming model from start, worry about horizontal scalability later.
- Optimize for Ops
  - Monitoring: critical for long running jobs.
  - Ease deployment --- single binary, different saw / driver configuration on different nodes, update and restart the whole thing in one shot.
  - Pipeline / data dependency management and scheduling.
- Cloud infrastructures
  - Cheap network storage like S3 or GCS can be as fast as local SSD. See also [Latency Numbers Every programmer Should Know](http://www.eecs.berkeley.edu/~rcs/research/interactive_latency.html)
  - Off-the-shelf haddop + spark cluster.
  - Computer instances can be start / stop within minutes, and even docker and kubernetes, oh my...
  - Saw won't re-invent the wheel for all these stuff already there.

### Local instance

### Tables

### Distributed computing

### Persistency / Storage

### Fault tolerence

## Example: session + dashboard
