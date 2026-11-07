package org.hadooptraffic.mapper;

import org.apache.hadoop.fs.Path;

/** Table names and file layout: {@code <dir>/<table>/part-<5-digit index>.<ext>}. */
public final class Tables {
  public static final String INODE = "inode";
  public static final String EDGE = "edge";
  public static final String BLOCK_INODE = "block_inode";
  public static final String META = "meta";

  private Tables() {
  }

  public static Path file(Path dir, String table, String ext) {
    return file(dir, table, 0, ext);
  }

  public static Path file(Path dir, String table, int part, String ext) {
    return new Path(new Path(dir, table), String.format("part-%05d.%s", part, ext));
  }
}
