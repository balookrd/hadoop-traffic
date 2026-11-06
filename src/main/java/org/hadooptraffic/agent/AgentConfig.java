package org.hadooptraffic.agent;

/** Parsed -javaagent option string: comma-separated key=value pairs. */
public final class AgentConfig {
  public static final int DEFAULT_MAX_USERS = 2000;

  private final int maxUsers;

  private AgentConfig(int maxUsers) {
    this.maxUsers = maxUsers;
  }

  public int maxUsers() {
    return maxUsers;
  }

  public static AgentConfig parse(String args) {
    int maxUsers = DEFAULT_MAX_USERS;
    if (args != null) {
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
        if ("maxUsers".equals(key)) {
          maxUsers = Integer.parseInt(value);
          if (maxUsers < 1) {
            throw new IllegalArgumentException("maxUsers must be >= 1: " + maxUsers);
          }
        } else {
          throw new IllegalArgumentException("Unknown agent option: " + key);
        }
      }
    }
    return new AgentConfig(maxUsers);
  }
}
