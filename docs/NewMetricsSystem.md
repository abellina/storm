# Enhanced Storm Stats Project

This is a proposal to redesign Apache Storm statistics generation, transfer, and collection. 

## Current State of Storm Stats

There are three main ways statiscs currently flow through Apache Storm (as of 2016):

1. Workers can generate stats (using?) and a topology can subscribe to them using IMetricsConsumer. Describe how this works.. Chart..
    * This comes as a tuple under `METRICS_STREAM_ID`.
    * Spout/BoltExecutor both trigger Executor::metricsTick from tupleActionFn
    * metricsTick call getValueAndReset on each individual metric
    * then it sends the results using sendUnanchored over METRICS_STREAM_ID. This puts the metrics on the transfer queue.
    * MetricsConsumerBolt takes a metrics tuple (from the execute method) and puts it in a queue, the IMetricsConsumer is known to this bolt, and it gets called when the blocking queue gets a stat asynchronously (handleDataPoints)
    * Additionally, Nimbus send cluster-metrics to the IMetricsConsumer at an interval (STORM_CLUSTER_METRICS_CONSUMER_PUBLISH_INTERVAL_SECS), it also sends supervisor-metrics. Cluster-info and supervisor-info stats come from core/ui code.
   
2. Workers collect executor statistics and send to Zookeeper. Describe how this works... Chart..
    * As part of heartbeats, workers add statistics to the heartbeat sent to ZK (StatsUtil/convertExecutorZkHbs). Foreach executor heartbeat, take ExecutorStats, and return a map.
    * Apparently all info about executors in the worker heartbeats is because of stats, so we need to maintain part of that
      The heartbeat looks like: { executor-stats, uptime, time_secs }, needs to be come likely { executors, uptime, time_secs }
    * A Spout/BoltExecutor has a Spout/BoltExecutorStats member. This subclasses from CommonStats. This is what accumulates the metrics for things like TRANSFERRED, EMITTED, and so on.

3. Daemons such as Nimbus, UI, and Supervisor implement very basic codahale metrics. We are looking to expand the adoption of codahale throuhgout. Describe how this works... Chart..
4. storm-metrics in external and Sigar?
5. eventhub?

The UI consumes executor statistics, does it consume IMetricsConsumer? It does this through Zookeeper. One issue with the current approach is that Zooekeeper isn't the place for metrics, and we could free it up to do what it is designed to do.

## Redesign Proposal (High-Level Requirements)

"For the context of this post we want to use Graphite to track the number of received tuples of an example bolt per node in the Storm cluster."

How to accomplish that?

A storm component can run many times in a JVM, let alone across the cluster. How do we combine?

How to send the metrics from each JVM to codahale. Are we adding a new communication path? Do I just use thrift against Nimbus?

1. We need to decide on a way to send metrics to codahale aggregators. The way I see it we should have a daemon per Node that is the point of contact by all workers, that way all workers talk to localhost. Then, that daemon talks to directly to something like Graphite. I don't see a lot of use in having Nimbus involved in this part. But maybe I am wrong. JStorm goes up the hierarchy until reqching Nimbus.
2. We can turn all metrics to codahale metrics as a project on its own. Define how user creates their own.
3. Out of the box, we can store these metrics into RocksDB (pluggable). That's where we are focusing for now.

### Out-of-the-box Changes

0. For each stat found before, figure out how the need to be aggregated. Propose namespaces.
    1. Suggest other metrics
    2. Suggest ways to manage namespaces by users. Can namespaces be locked?
1. IMetricsConsumer. How do we make this part of the RocksDB impl?
2. Executor stats. How do we send these to Nimbus/RocksDB?
3. Existing codahale work. How do we route to Nimbus/RocksDB? 
4. How to plugin JMX to all of this?

### Codahale Library Usage

1. Codahale in daemons, how do we expand. Do codahale in daemons deal with codahale in workers and user code?
2. How users can interact with this, we really want users to generate their own stats alongside storm defaults.
3. Perhaps we can recomend a distrubited graphite setup using carbon-relay, and multiple carbon-cache nodes

### Milestones
