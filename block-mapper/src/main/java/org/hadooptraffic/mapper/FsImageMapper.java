package org.hadooptraffic.mapper;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Reads a downloaded fsimage and writes the inode model to {@code -o}. Writes into
 * the hidden sibling {@code <parent>/.<name>._tmp} and renames it to {@code <out>} only on success.
 *
 * <pre>
 * hdfs dfsadmin -fetchImage /data/fsimg
 * hadoop jar block-mapper-&lt;ver&gt;.jar org.hadooptraffic.mapper.FsImageMapper \
 *   -i /data/fsimg/fsimage_&lt;txid&gt; -o hdfs:///data/hdfs-traffic/fsimage/dt=&lt;date&gt;
 * </pre>
 */
public final class FsImageMapper {
  private FsImageMapper() {
  }

  public static void main(String[] args) {
    System.exit(run(args, new Configuration(), System.err));
  }

  /** @return 0 on success, 1 on failure, 2 on a usage error */
  public static int run(String[] args, Configuration conf, PrintStream err) {
    MapperOptions opts;
    try {
      opts = MapperOptions.parse(args);
    } catch (IllegalArgumentException e) {
      err.println("block-mapper: " + e.getMessage());
      return 2;
    }
    Path out;
    Path tmp;
    try {
      out = new Path(opts.output());
      if (out.getParent() == null) {
        throw new IllegalArgumentException("-o must have a parent directory: " + opts.output());
      }
      // Hidden sibling: a "dt=..._tmp" name would look like a partition to Spark and Hive.
      tmp = new Path(out.getParent(), "." + out.getName() + "._tmp");
    } catch (IllegalArgumentException e) {
      err.println("block-mapper: " + e.getMessage());
      return 2;
    }
    File image = new File(opts.image());
    if (!image.isFile()) {
      err.println("block-mapper: fsimage not found: " + opts.image());
      return 1;
    }
    FileSystem fs;
    try {
      fs = out.getFileSystem(conf);
      if (fs.exists(out)) {
        err.println("block-mapper: output already exists: " + out);
        return 1;
      }
      fs.delete(tmp, true);
    } catch (IOException e) {
      err.println("block-mapper: " + e);
      return 1;
    }
    try {
      try (InodeModelSink sink = opts.format() == MapperOptions.Format.ORC
          ? new OrcModelSink(conf, tmp, opts.threads())
          : new TsvModelSink(fs, tmp)) {
        new FsImageReader(conf).read(image, sink);
      }
      if (!fs.rename(tmp, out)) {
        throw new IOException("cannot rename " + tmp + " to " + out);
      }
      return 0;
    } catch (IOException | RuntimeException e) {
      err.println("block-mapper: " + e);
      try {
        fs.delete(tmp, true);
      } catch (IOException ignored) {
        // Best effort: the message above already reports the failure.
      }
      return 1;
    }
  }
}
