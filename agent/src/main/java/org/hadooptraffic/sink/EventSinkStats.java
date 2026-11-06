package org.hadooptraffic.sink;

/** Health counters of the events sink, published via JMX. */
public interface EventSinkStats {
  long eventsWritten();

  long eventsDropped();

  long queueSize();

  long filesDropped();
}
