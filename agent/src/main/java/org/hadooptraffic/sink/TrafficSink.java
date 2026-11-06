package org.hadooptraffic.sink;

import org.hadooptraffic.model.TrafficEvent;

/** Receives finished operations. Implementations must be thread-safe and fast. */
public interface TrafficSink {
  void onEvent(TrafficEvent event);
}
