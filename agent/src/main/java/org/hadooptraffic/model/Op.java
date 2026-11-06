package org.hadooptraffic.model;

/** Kind of DataNode operation being accounted. */
public enum Op {
  READ("read"),
  WRITE("write"),
  TRANSFER("transfer"),
  COPY("copy"),
  REPLACE("replace"),
  SCR_FDS("scr_fds"),
  WEBHDFS_READ("webhdfs_read"),
  WEBHDFS_CREATE("webhdfs_create"),
  WEBHDFS_APPEND("webhdfs_append");

  private final String label;

  Op(String label) {
    this.label = label;
  }

  /** Stable lower-case name used in metric names. */
  public String label() {
    return label;
  }

  /** Maps a DataXceiver method name to an op, or null if the method is not accounted. */
  public static Op fromXceiverMethod(String method) {
    if (method == null) {
      return null;
    }
    switch (method) {
      case "readBlock": return READ;
      case "writeBlock": return WRITE;
      case "transferBlock": return TRANSFER;
      case "copyBlock": return COPY;
      case "replaceBlock": return REPLACE;
      case "requestShortCircuitFds": return SCR_FDS;
      default: return null;
    }
  }

  /** Maps a WebHdfsHandler method name to an op, or null if the method is not accounted. */
  public static Op fromWebHdfsMethod(String method) {
    if (method == null) {
      return null;
    }
    switch (method) {
      case "onOpen": return WEBHDFS_READ;
      case "onCreate": return WEBHDFS_CREATE;
      case "onAppend": return WEBHDFS_APPEND;
      default: return null;
    }
  }
}
