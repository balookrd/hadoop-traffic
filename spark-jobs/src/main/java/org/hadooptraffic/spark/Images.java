package org.hadooptraffic.spark;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Finds block-mapper outputs under fsimage/. */
public final class Images {
  private static final Pattern DT_DIR = Pattern.compile("dt=(\\d{4}-\\d{2}-\\d{2})");

  private Images() {
  }

  /** Latest dt=YYYY-MM-DD directory whose meta/ holds an ORC file (complete mapper output). */
  public static String latest(FileSystem fs, Path fsimageRoot) {
    String best = null;
    try {
      FileStatus[] dirs = fs.exists(fsimageRoot) ? fs.listStatus(fsimageRoot) : new FileStatus[0];
      for (FileStatus d : dirs) {
        java.util.regex.Matcher m = DT_DIR.matcher(d.getPath().getName());
        if (!d.isDirectory() || !m.matches()) {
          continue;
        }
        FileStatus[] meta = fs.globStatus(new Path(d.getPath(), "meta/*.orc"));
        if (meta != null && meta.length > 0 && (best == null || m.group(1).compareTo(best) > 0)) {
          best = m.group(1);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("cannot list " + fsimageRoot + ": " + e, e);
    }
    if (best == null) {
      throw new IllegalStateException("no fsimage under " + fsimageRoot
          + " (run block-mapper first)");
    }
    return best;
  }
}
