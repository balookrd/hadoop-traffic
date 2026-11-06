package org.hadooptraffic.agent;

/** Logs via slf4j when available (always on a DataNode), otherwise to stderr. */
public final class AgentLog {
  private static final String NAME = "org.hadooptraffic.agent";

  private AgentLog() {
  }

  public static void info(String msg) {
    try {
      org.slf4j.LoggerFactory.getLogger(NAME).info(msg);
    } catch (Throwable t) {
      System.err.println("[hdfs-traffic-agent] INFO " + msg);
    }
  }

  public static void warn(String msg, Throwable error) {
    try {
      org.slf4j.LoggerFactory.getLogger(NAME).warn(msg, error);
    } catch (Throwable t) {
      System.err.println("[hdfs-traffic-agent] WARN " + msg);
      if (error != null) {
        error.printStackTrace();
      }
    }
  }
}
