package org.hadooptraffic.it;

import java.util.Map;
import java.util.TreeMap;
import org.hadooptraffic.agent.TrafficAgent;
import org.hadooptraffic.aggregate.TrafficCounters;
import org.hadooptraffic.aggregate.TrafficKey;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/** Reads the agent's aggregator, summing over localities. */
public final class TrafficAssert {
  private TrafficAssert() {
  }

  public static long bytes(String user, Op op, Role role) {
    long sum = 0;
    for (Map.Entry<TrafficKey, TrafficCounters.Snapshot> e : snapshot().entrySet()) {
      if (matches(e.getKey(), user, op, role)) {
        sum += e.getValue().getBytes();
      }
    }
    return sum;
  }

  public static long ops(String user, Op op, Role role) {
    long sum = 0;
    for (Map.Entry<TrafficKey, TrafficCounters.Snapshot> e : snapshot().entrySet()) {
      if (matches(e.getKey(), user, op, role)) {
        sum += e.getValue().getOps();
      }
    }
    return sum;
  }

  public static String dump() {
    Map<String, String> sorted = new TreeMap<>();
    for (Map.Entry<TrafficKey, TrafficCounters.Snapshot> e : snapshot().entrySet()) {
      TrafficCounters.Snapshot s = e.getValue();
      sorted.put(e.getKey().toString(), "bytes=" + s.getBytes() + " est="
          + s.getEstimatedBytes() + " ops=" + s.getOps());
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> e : sorted.entrySet()) {
      sb.append("  ").append(e.getKey()).append(": ").append(e.getValue()).append('\n');
    }
    return sb.toString();
  }

  private static Map<TrafficKey, TrafficCounters.Snapshot> snapshot() {
    return TrafficAgent.aggregator().snapshot();
  }

  private static boolean matches(TrafficKey k, String user, Op op, Role role) {
    return k.getUser().equals(user) && k.getOp() == op && k.getRole() == role;
  }
}
