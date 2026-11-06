package org.hadooptraffic.agent;

import java.lang.instrument.Instrumentation;
import org.hadooptraffic.aggregate.TrafficAggregator;
import org.hadooptraffic.metrics.MetricsRegistrar;
import org.hadooptraffic.sink.TrafficSinks;

/** Entry point: -javaagent:hdfs-traffic-agent.jar[=maxUsers=N]. */
public final class TrafficAgent {
  private static volatile TrafficAggregator aggregator;
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
      TrafficAggregator agg = new TrafficAggregator(config.maxUsers());
      TrafficSinks.register(agg);
      MetricsRegistrar.init(agg);
      aggregator = agg;
      Instrumentations.builder().installOn(inst);
      AgentLog.info("hdfs-traffic-agent installed (maxUsers=" + config.maxUsers() + ")");
    } catch (Throwable t) {
      AgentLog.warn("hdfs-traffic-agent disabled: " + t, t);
    }
  }

  /** The aggregator, or null if the agent is not installed. */
  public static TrafficAggregator aggregator() {
    return aggregator;
  }
}
