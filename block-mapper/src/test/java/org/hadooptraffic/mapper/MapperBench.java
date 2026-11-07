package org.hadooptraffic.mapper;

import java.io.File;
import org.apache.hadoop.conf.Configuration;

/**
 * Runs {@link FsImageMapper} on an image and prints wall time, CPU time and peak heap. Not a
 * test (the name matches no surefire include); run it by hand on a {@link SyntheticImage}:
 *
 * <pre>
 * java -Xmx4g -cp ... org.hadooptraffic.mapper.MapperBench /tmp/fsimage_... file:///tmp/out/dt=x [--threads N]
 * java -Xmx4g -cp ... org.hadooptraffic.mapper.MapperBench /tmp/fsimage_... null   # reader only
 * </pre>
 */
final class MapperBench {
  private MapperBench() {
  }

  public static void main(String[] args) throws Exception {
    String image = args[0];
    String out = args[1];
    PeakHeap peak = new PeakHeap();
    peak.start();
    long cpu0 = processCpuNanos();
    long t0 = System.nanoTime();
    int rc;
    if ("null".equals(out)) {
      // Parse only: the sink discards everything.
      new FsImageReader(new Configuration()).read(new File(image), new NullSink());
      rc = 0;
    } else {
      String[] mapperArgs = new String[4 + args.length - 2];
      mapperArgs[0] = "-i";
      mapperArgs[1] = image;
      mapperArgs[2] = "-o";
      mapperArgs[3] = out;
      System.arraycopy(args, 2, mapperArgs, 4, args.length - 2);
      rc = FsImageMapper.run(mapperArgs, new Configuration(), System.err);
    }
    double wall = (System.nanoTime() - t0) / 1e9;
    double cpu = (processCpuNanos() - cpu0) / 1e9;
    peak.interrupt();
    peak.join();
    System.out.printf("rc=%d wall=%.1fs cpu=%.1fs peakHeap=%dMB image=%dMB%n", rc, wall, cpu,
        peak.max >> 20, new File(image).length() >> 20);
    System.exit(rc);
  }

  /** Discards the model; for timing the reader alone. */
  private static final class NullSink implements InodeModelSink {
    @Override
    public void inode(long id, char type, String name) {
    }

    @Override
    public void block(long blockId, long inodeId) {
    }

    @Override
    public void edge(long childId, long parentId) {
    }

    @Override
    public void meta(long txid, String imageFile, long parsedAtMillis) {
    }

    @Override
    public void close() {
    }
  }

  private static long processCpuNanos() {
    java.lang.management.OperatingSystemMXBean os =
        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    return ((com.sun.management.OperatingSystemMXBean) os).getProcessCpuTime();
  }

  /** Samples used heap every 100 ms. */
  private static final class PeakHeap extends Thread {
    volatile long max;

    PeakHeap() {
      setDaemon(true);
    }

    @Override
    public void run() {
      Runtime rt = Runtime.getRuntime();
      while (!isInterrupted()) {
        max = Math.max(max, rt.totalMemory() - rt.freeMemory());
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
}
