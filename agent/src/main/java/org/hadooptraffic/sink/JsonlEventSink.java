package org.hadooptraffic.sink;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.hadooptraffic.agent.AgentLog;
import org.hadooptraffic.context.Clock;
import org.hadooptraffic.model.TrafficEvent;

/**
 * Writes events as JSON lines to a rotating local file. {@link #onEvent} only offers to a
 * bounded queue (dropping when full); a single daemon thread formats and writes.
 */
public final class JsonlEventSink implements TrafficSink, EventSinkStats {
  private static final int BATCH = 4096;
  private static final long WARN_INTERVAL_MS = 60_000L;
  private static final long POLL_MS = 200L;

  private final Path activeFile;
  private final BlockingQueue<TrafficEvent> queue;
  private final RollingEventFile file;
  private final Supplier<String> dnSupplier;
  private String dn; // resolved by the writer thread before its first batch
  private final Clock clock;
  private final Thread writer;
  private final AtomicLong offered = new AtomicLong();
  private final AtomicLong processed = new AtomicLong();
  private final AtomicLong written = new AtomicLong();
  private final AtomicLong dropped = new AtomicLong();
  private volatile boolean closing;
  private volatile long lastWarnMillis = Long.MIN_VALUE / 2;

  public JsonlEventSink(Path activeFile, int queueCapacity, long maxFileBytes, long rotateMillis,
      int maxFiles, Supplier<String> dnSupplier, Clock clock) {
    this.activeFile = activeFile;
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
    this.file = new RollingEventFile(activeFile, maxFileBytes, rotateMillis, maxFiles, clock);
    this.dnSupplier = dnSupplier;
    this.clock = clock;
    this.writer = new Thread(this::run, "hdfs-traffic-events");
    this.writer.setDaemon(true);
  }

  public void start() {
    writer.start();
  }

  public Path activeFile() {
    return activeFile;
  }

  @Override
  public void onEvent(TrafficEvent e) {
    if (closing) {
      dropped.incrementAndGet();
      return;
    }
    if (queue.offer(e)) {
      offered.incrementAndGet();
    } else {
      dropped.incrementAndGet();
    }
  }

  /** Host name for the "dn" field; resolved on the writer thread so premain never waits on DNS. */
  private String resolveDn() {
    try {
      String name = dnSupplier.get();
      return name == null ? "unknown" : name;
    } catch (Throwable e) {
      return "unknown";
    }
  }

  private void run() {
    dn = resolveDn();
    try {
      file.rotate(); // a crash leftover is closed out now, not on the first write
    } catch (Throwable e) {
      warnRateLimited(e);
    }
    List<TrafficEvent> batch = new ArrayList<>(BATCH);
    while (!closing || !queue.isEmpty()) {
      try {
        TrafficEvent first = queue.poll(POLL_MS, TimeUnit.MILLISECONDS);
        if (first != null) {
          batch.add(first);
          queue.drainTo(batch, BATCH - 1);
        }
        writeBatch(batch);
      } catch (InterruptedException e) {
        // Only close() stops the writer; a stray interrupt must not.
      } catch (Throwable t) {
        warnRateLimited(t); // the writer thread must never die
      } finally {
        processed.addAndGet(batch.size());
        batch.clear();
      }
    }
  }

  private void writeBatch(List<TrafficEvent> batch) {
    int ok = 0;
    int bad = 0;
    try {
      for (TrafficEvent e : batch) {
        String line;
        try {
          line = EventJson.format(e, dn);
        } catch (RuntimeException ex) {
          dropped.incrementAndGet(); // only this event is lost
          bad++;
          warnRateLimited(ex);
          continue;
        }
        file.write(line, e.getTimestampMillis());
        ok++;
      }
      file.flush();
    } catch (IOException | RuntimeException e) {
      dropped.addAndGet(batch.size() - bad);
      file.abandon();
      warnRateLimited(e);
      return;
    }
    written.addAndGet(ok);
    try {
      file.maybeRotate();
    } catch (IOException | RuntimeException e) {
      file.abandon();
      warnRateLimited(e);
    }
  }

  private void warnRateLimited(Throwable t) {
    long now = clock.currentTimeMillis();
    if (now - lastWarnMillis >= WARN_INTERVAL_MS) {
      lastWarnMillis = now;
      AgentLog.warn("hdfs-traffic-agent: cannot write events to " + activeFile
          + "; dropping events until it recovers", t);
    }
  }

  /** Waits until every accepted event is written (or dropped) and flushed. For tests. */
  public boolean awaitIdle(long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (processed.get() < offered.get()) {
      if (System.currentTimeMillis() > deadline) {
        return false;
      }
      Thread.sleep(10);
    }
    return true;
  }

  /** Drains the queue for at most timeoutMs, then closes and rotates the active file. */
  public void close(long timeoutMs) {
    closing = true;
    if (writer.isAlive()) {
      try {
        writer.join(timeoutMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (writer.isAlive()) {
      return; // still writing: the file is not ours to touch
    }
    // Offered after the writer's last check, or never written because it was not started.
    int left = queue.size();
    queue.clear();
    if (left > 0) {
      dropped.addAndGet(left);
      processed.addAndGet(left);
    }
    try {
      file.close();
    } catch (IOException e) {
      warnRateLimited(e);
    }
  }

  @Override
  public long eventsWritten() {
    return written.get();
  }

  @Override
  public long eventsDropped() {
    return dropped.get();
  }

  @Override
  public long queueSize() {
    return queue.size();
  }

  @Override
  public long filesDropped() {
    return file.filesDropped();
  }
}
