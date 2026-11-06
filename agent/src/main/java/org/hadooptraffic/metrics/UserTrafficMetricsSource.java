package org.hadooptraffic.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.aggregate.TrafficCounters;
import org.hadooptraffic.aggregate.TrafficKey;
import org.hadooptraffic.sink.EventSinkStats;

/**
 * Publishes aggregator counters as one metrics2 record. Dimensions are encoded into metric
 * names ("Bytes.user.u.op.o.role.r.locality.l", nntop style) because metrics2 flattens
 * multi-record sources into suffixed JMX attributes and Ambari Metrics ignores tags.
 *
 * <p>Names never contain '=', ':' or ' ': Hadoop's /jmx servlet silently skips such
 * attributes, although the MBeanServer itself exposes them.
 */
public final class UserTrafficMetricsSource implements MetricsSource {
  public static final String RECORD = "UserTraffic";
  private static final MetricName RECORD_INFO =
      new MetricName(RECORD, "Per-user DataNode traffic");

  private final TrafficAggregator aggregator;
  private final EventSinkStats sinkStats;

  /**
   * @param aggregator per-user counters, or null when userMetrics is off
   * @param sinkStats events sink health, or null when the sink is off
   */
  public UserTrafficMetricsSource(TrafficAggregator aggregator, EventSinkStats sinkStats) {
    this.aggregator = aggregator;
    this.sinkStats = sinkStats;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(RECORD_INFO).setContext("dfs");
    if (aggregator != null) {
      for (Map.Entry<String, Long> e : metricValues(aggregator.snapshot()).entrySet()) {
        rb.addCounter(new MetricName(e.getKey(), describe(e.getKey())), e.getValue());
      }
    }
    if (sinkStats != null) {
      for (Map.Entry<String, Long> e : sinkValues(sinkStats).entrySet()) {
        MetricName name = new MetricName(e.getKey(), describe(e.getKey()));
        if ("EventsQueueSize".equals(e.getKey())) {
          rb.addGauge(name, e.getValue());
        } else {
          rb.addCounter(name, e.getValue());
        }
      }
    }
  }

  /** Global health counters of the events sink. */
  public static Map<String, Long> sinkValues(EventSinkStats s) {
    Map<String, Long> out = new LinkedHashMap<>();
    out.put("EventsWritten", s.eventsWritten());
    out.put("EventsDropped", s.eventsDropped());
    out.put("EventFilesDropped", s.filesDropped());
    out.put("EventsQueueSize", s.queueSize());
    return out;
  }

  /** Flattens a snapshot into metric name -> value. */
  public static Map<String, Long> metricValues(Map<TrafficKey, TrafficCounters.Snapshot> snap) {
    Map<String, Long> out = new LinkedHashMap<>();
    for (Map.Entry<TrafficKey, TrafficCounters.Snapshot> e : snap.entrySet()) {
      TrafficKey k = e.getKey();
      TrafficCounters.Snapshot s = e.getValue();
      out.put(attributeName("Bytes", k), s.getBytes());
      out.put(attributeName("EstimatedBytes", k), s.getEstimatedBytes());
      out.put(attributeName("Ops", k), s.getOps());
    }
    return out;
  }

  public static String attributeName(String counter, TrafficKey k) {
    return counter + ".user." + jmxSafe(k.getUser()) + ".op." + k.getOp().label()
        + ".role." + k.getRole().label() + ".locality." + k.getLocality().label();
  }

  /** Replaces the characters JMXJsonServlet rejects in attribute names. */
  private static String jmxSafe(String s) {
    return s.replace('=', '_').replace(':', '_').replace(' ', '_');
  }

  private static String describe(String name) {
    switch (name) {
      case "EventsWritten":
        return "Events written to the local JSONL file";
      case "EventsDropped":
        return "Events lost: full queue or I/O error";
      case "EventFilesDropped":
        return "Closed event files deleted before shipping (eventsMaxFiles)";
      case "EventsQueueSize":
        return "Events waiting to be written";
      default:
        break;
    }
    if (name.startsWith("EstimatedBytes.")) {
      return "Estimated bytes (short-circuit reads, not observed on the wire)";
    }
    if (name.startsWith("Bytes.")) {
      return "Payload bytes transferred";
    }
    return "Operations";
  }
}
