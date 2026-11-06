package org.hadooptraffic.spark;

import java.util.regex.Pattern;

/**
 * Folds a file path to the directory of its table without the Hive metastore: drop the file
 * name, then cut at the first partition ({@code key=value}), service ({@code .x}, {@code _x})
 * or Hive ACID ({@code base_N}, {@code delta_N_N}, {@code delete_delta_N_N}) component.
 */
public final class TableDir {
  public static final String UNKNOWN = "?";

  private static final Pattern ACID = Pattern.compile(
      "(base_\\d+|delta_\\d+_\\d+(_\\d+)?|delete_delta_\\d+_\\d+(_\\d+)?)(_v\\d+)?");

  private TableDir() {
  }

  public static String of(String path) {
    if (path == null || path.isEmpty() || path.charAt(0) != '/') {
      return UNKNOWN;
    }
    String[] parts = path.split("/");
    StringBuilder dir = new StringBuilder();
    // parts[0] is "" (leading slash); the last part is the file name.
    for (int i = 1; i < parts.length - 1; i++) {
      if (isCut(parts[i])) {
        break;
      }
      dir.append('/').append(parts[i]);
    }
    return dir.length() == 0 ? "/" : dir.toString();
  }

  static boolean isCut(String c) {
    return c.isEmpty() || c.charAt(0) == '.' || c.charAt(0) == '_' || c.indexOf('=') > 0
        || ACID.matcher(c).matches();
  }
}
