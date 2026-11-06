package org.hadooptraffic.sink;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.hadooptraffic.model.TrafficEvent;

/** Process-wide fan-out of events to registered sinks. */
public final class TrafficSinks {
  private static final List<TrafficSink> SINKS = new CopyOnWriteArrayList<>();

  private TrafficSinks() {
  }

  public static void register(TrafficSink sink) {
    SINKS.add(Objects.requireNonNull(sink, "sink"));
  }

  /** Removes all sinks. Intended for tests. */
  public static void clear() {
    SINKS.clear();
  }

  public static void emit(TrafficEvent event) {
    for (TrafficSink sink : SINKS) {
      try {
        sink.onEvent(event);
      } catch (Throwable e) {
        // A faulty sink must never break a DataNode operation or starve other sinks.
      }
    }
  }
}
