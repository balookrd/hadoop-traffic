package org.hadooptraffic.agent;

import java.lang.instrument.Instrumentation;
import java.net.InetAddress;
import java.nio.file.Paths;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.context.Clock;
import org.hadooptraffic.context.OpContext;
import org.hadooptraffic.metrics.MetricsRegistrar;
import org.hadooptraffic.sink.JsonlEventSink;
import org.hadooptraffic.sink.TrafficSinks;

/** Entry point: -javaagent:hdfs-traffic-agent.jar[=key=value,...]. */
public final class TrafficAgent {
  private static volatile TrafficAggregator aggregator;
  private static volatile JsonlEventSink eventSink;
  private static boolean installed;

  private TrafficAgent() {
  }

  public static void premain(String args, Instrumentation inst) {
    install(args, inst);
  }

  public static void agentmain(String args, Instrumentation inst) {
    install(args, inst);
  }

  /** Idempotent. Never throws: any failure disables the agent and is logged. */
  public static synchronized void install(String args, Instrumentation inst) {
    if (installed) {
      return;
    }
    installed = true;
    try {
      AgentConfig config = AgentConfig.parse(args);
      // Instrument first: if this fails nothing else has been started or registered.
      Instrumentations.builder().installOn(inst);
      OpContext.setFlushIntervalSec(config.flushIntervalSec());
      // Off by default: per-user counters reach ~60k JMX attributes per DataNode.
      TrafficAggregator agg =
          config.userMetrics() ? new TrafficAggregator(config.maxUsers()) : null;
      JsonlEventSink sink = null;
      if (config.eventsPath() != null) {
        sink = new JsonlEventSink(Paths.get(config.eventsPath()), config.eventsQueue(),
            config.eventsMaxFileMb() * 1024L * 1024L, config.eventsRotateSec() * 1000L,
            config.eventsMaxFiles(), TrafficAgent::localHostName, Clock.SYSTEM);
        JsonlEventSink toClose = sink;
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> toClose.close(2000), "hdfs-traffic-events-shutdown"));
        sink.start();
      }
      if (agg != null) {
        TrafficSinks.register(agg);
      }
      if (sink != null) {
        TrafficSinks.register(sink);
      }
      MetricsRegistrar.init(agg, sink);
      aggregator = agg;
      eventSink = sink;
      if (agg == null && sink == null) {
        AgentLog.warn("hdfs-traffic-agent: userMetrics=false and no events path: "
            + "traffic is not recorded anywhere", null);
      }
      AgentLog.info("hdfs-traffic-agent installed (userMetrics=" + config.userMetrics()
          + (agg == null ? "" : ", maxUsers=" + config.maxUsers())
          + ", events=" + (sink == null ? "off" : config.eventsPath())
          + ", flushIntervalSec=" + config.flushIntervalSec() + ")");
    } catch (Throwable t) {
      AgentLog.warn("hdfs-traffic-agent disabled: " + t, t);
    }
  }

  private static String localHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception e) {
      return "unknown";
    }
  }

  /** The aggregator, or null if the agent is not installed or userMetrics is off. */
  public static TrafficAggregator aggregator() {
    return aggregator;
  }

  /** The JSONL events sink, or null if the agent is not installed or events are off. */
  public static JsonlEventSink eventSink() {
    return eventSink;
  }
}
