package org.hadooptraffic.aggregate;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.hadooptraffic.model.TrafficEvent;
import org.hadooptraffic.sink.TrafficSink;

/** Sink that keeps cumulative counters per (user, op, role, locality). */
public final class TrafficAggregator implements TrafficSink {
  public static final String OTHER_USER = "__other__";

  private final int maxUsers;
  private final ConcurrentHashMap<TrafficKey, TrafficCounters> counters =
      new ConcurrentHashMap<>();
  private final Set<String> users = ConcurrentHashMap.newKeySet();

  public TrafficAggregator(int maxUsers) {
    if (maxUsers < 1) {
      throw new IllegalArgumentException("maxUsers must be >= 1: " + maxUsers);
    }
    this.maxUsers = maxUsers;
  }

  @Override
  public void onEvent(TrafficEvent e) {
    TrafficKey key = new TrafficKey(admitUser(e.getUser()), e.getOp(), e.getRole(),
        e.getLocality());
    counters.computeIfAbsent(key, k -> new TrafficCounters())
        .record(e.getBytes(), e.getEstimatedBytes(), e.isLast());
  }

  /** Returns the user itself while under the cap, otherwise {@link #OTHER_USER}. */
  private String admitUser(String user) {
    if (users.contains(user)) {
      return user;
    }
    synchronized (users) {
      if (users.contains(user)) {
        return user;
      }
      if (users.size() >= maxUsers) {
        return OTHER_USER;
      }
      users.add(user);
      return user;
    }
  }

  public Map<TrafficKey, TrafficCounters.Snapshot> snapshot() {
    Map<TrafficKey, TrafficCounters.Snapshot> out = new HashMap<>();
    for (Map.Entry<TrafficKey, TrafficCounters> e : counters.entrySet()) {
      out.put(e.getKey(), e.getValue().snapshot());
    }
    return Collections.unmodifiableMap(out);
  }

  /** Distinct users currently tracked (excluding {@link #OTHER_USER}). */
  public Set<String> trackedUsers() {
    return Collections.unmodifiableSet(new HashSet<>(users));
  }
}
