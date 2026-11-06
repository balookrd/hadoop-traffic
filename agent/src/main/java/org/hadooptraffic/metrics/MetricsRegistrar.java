package org.hadooptraffic.metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.hadooptraffic.agent.AgentLog;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.sink.EventSinkStats;

/** Registers the metrics source once per JVM, after the DataNode metrics system is up. */
public final class MetricsRegistrar {
  public static final String SOURCE_NAME = "UserTraffic";

  private static volatile TrafficAggregator aggregator;
  private static volatile EventSinkStats sinkStats;
  private static final AtomicBoolean REGISTERED = new AtomicBoolean();

  private MetricsRegistrar() {
  }

  /** Called by the agent at install time; sinkStats is null when the events sink is off. */
  public static void init(TrafficAggregator agg, EventSinkStats stats) {
    aggregator = agg;
    sinkStats = stats;
  }

  /** Called on exit of DataNode.startDataNode; later calls are no-ops. */
  public static void registerOnce() {
    TrafficAggregator agg = aggregator;
    if (agg == null || !REGISTERED.compareAndSet(false, true)) {
      return;
    }
    try {
      DefaultMetricsSystem.instance().register(SOURCE_NAME, "Per-user DataNode traffic",
          new UserTrafficMetricsSource(agg, sinkStats));
      AgentLog.info("hdfs-traffic-agent: UserTraffic metrics source registered");
    } catch (Throwable t) {
      AgentLog.warn("hdfs-traffic-agent: UserTraffic metrics source not registered; "
          + "no JMX metrics will be published", t);
    }
  }
}
