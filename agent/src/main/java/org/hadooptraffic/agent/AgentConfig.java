package org.hadooptraffic.agent;

/** Parsed -javaagent option string: comma-separated key=value pairs. */
public final class AgentConfig {
  public static final int DEFAULT_MAX_USERS = 2000;
  public static final int DEFAULT_EVENTS_QUEUE = 65536;
  public static final int DEFAULT_EVENTS_MAX_FILE_MB = 256;
  public static final int DEFAULT_EVENTS_ROTATE_SEC = 300;
  public static final int DEFAULT_EVENTS_MAX_FILES = 100;
  public static final int DEFAULT_FLUSH_INTERVAL_SEC = 10;

  private boolean userMetrics;
  private int maxUsers = DEFAULT_MAX_USERS;
  private String eventsPath;
  private int eventsQueue = DEFAULT_EVENTS_QUEUE;
  private int eventsMaxFileMb = DEFAULT_EVENTS_MAX_FILE_MB;
  private int eventsRotateSec = DEFAULT_EVENTS_ROTATE_SEC;
  private int eventsMaxFiles = DEFAULT_EVENTS_MAX_FILES;
  private int flushIntervalSec = DEFAULT_FLUSH_INTERVAL_SEC;

  private AgentConfig() {
  }

  /** Per-user JMX counters (UserTraffic); off by default because of their cardinality. */
  public boolean userMetrics() {
    return userMetrics;
  }

  public int maxUsers() {
    return maxUsers;
  }

  /** Active events file, or null when the JSONL sink is off. */
  public String eventsPath() {
    return eventsPath;
  }

  public int eventsQueue() {
    return eventsQueue;
  }

  public int eventsMaxFileMb() {
    return eventsMaxFileMb;
  }

  public int eventsRotateSec() {
    return eventsRotateSec;
  }

  public int eventsMaxFiles() {
    return eventsMaxFiles;
  }

  public int flushIntervalSec() {
    return flushIntervalSec;
  }

  public static AgentConfig parse(String args) {
    AgentConfig c = new AgentConfig();
    if (args == null) {
      return c;
    }
    for (String part : args.split(",")) {
      String p = part.trim();
      if (p.isEmpty()) {
        continue;
      }
      int eq = p.indexOf('=');
      if (eq < 0) {
        throw new IllegalArgumentException("Bad agent option (expected key=value): " + p);
      }
      String key = p.substring(0, eq).trim();
      String value = p.substring(eq + 1).trim();
      switch (key) {
        case "userMetrics":
          c.userMetrics = bool(key, value);
          break;
        case "maxUsers":
          c.maxUsers = atLeast(key, value, 1);
          break;
        case "events":
          if (value.isEmpty()) {
            throw new IllegalArgumentException("events must be a file path");
          }
          c.eventsPath = value;
          break;
        case "eventsQueue":
          c.eventsQueue = atLeast(key, value, 1);
          break;
        case "eventsMaxFileMb":
          c.eventsMaxFileMb = atLeast(key, value, 1);
          break;
        case "eventsRotateSec":
          c.eventsRotateSec = atLeast(key, value, 1);
          break;
        case "eventsMaxFiles":
          c.eventsMaxFiles = atLeast(key, value, 1);
          break;
        case "flushIntervalSec":
          c.flushIntervalSec = atLeast(key, value, 0);
          break;
        default:
          throw new IllegalArgumentException("Unknown agent option: " + key);
      }
    }
    return c;
  }

  private static boolean bool(String key, String value) {
    if ("true".equals(value)) {
      return true;
    }
    if ("false".equals(value)) {
      return false;
    }
    throw new IllegalArgumentException(key + " must be true or false: " + value);
  }

  private static int atLeast(String key, String value, int min) {
    int v;
    try {
      v = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer: " + value);
    }
    if (v < min) {
      throw new IllegalArgumentException(key + " must be >= " + min + ": " + v);
    }
    return v;
  }
}
