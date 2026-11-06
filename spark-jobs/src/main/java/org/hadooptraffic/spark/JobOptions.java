package org.hadooptraffic.spark;

import java.util.regex.Pattern;

/** Command line shared by both jobs. Values used in SQL are validated here. */
public final class JobOptions {
  public static final String USAGE = "usage: spark-submit --class <job> spark-jobs.jar "
      + "--dt YYYY-MM-DD [--image-dt YYYY-MM-DD] [--root <dir URI>] [--db <name>] "
      + "[--catalog hive|none]";

  /** Where partitions are registered. */
  public enum CatalogKind { HIVE, NONE }

  private static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
  private static final Pattern FS_ROOT =
      Pattern.compile("[A-Za-z][A-Za-z0-9+.-]*:(//[^/]*)?");
  private static final Pattern DB = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private String dt;
  private String imageDt;
  private String root = "/data/hdfs-traffic";
  private String db = "hdfs_traffic";
  private CatalogKind catalog = CatalogKind.HIVE;

  private JobOptions() {
  }

  public String dt() { return dt; }
  /** Image date, or null to use the latest image. */
  public String imageDt() { return imageDt; }
  public String root() { return root; }
  public String db() { return db; }
  public CatalogKind catalog() { return catalog; }

  public static JobOptions parse(String[] args) {
    JobOptions o = new JobOptions();
    for (int i = 0; i < args.length; i++) {
      String key = args[i];
      if (i + 1 >= args.length) {
        throw bad("missing value for " + key);
      }
      String value = args[++i];
      switch (key) {
        case "--dt":
          o.dt = date(key, value);
          break;
        case "--image-dt":
          o.imageDt = date(key, value);
          break;
        case "--root": {
          String r = value;
          while (r.length() > 0 && r.endsWith("/")) {
            r = r.substring(0, r.length() - 1);
          }
          // After stripping, "/" and "//" are empty; "hdfs:///" and "hdfs://nn:8020/" are a bare
          // scheme or authority.
          if (r.isEmpty() || FS_ROOT.matcher(r).matches() || r.indexOf('\'') >= 0
              || r.indexOf('\\') >= 0 || hasControl(r)) {
            throw bad("bad --root (empty or root of a file system): " + value);
          }
          o.root = r;
          break;
        }
        case "--db":
          if (!DB.matcher(value).matches()) {
            throw bad("bad --db: " + value);
          }
          o.db = value;
          break;
        case "--catalog":
          if ("hive".equals(value)) {
            o.catalog = CatalogKind.HIVE;
          } else if ("none".equals(value)) {
            o.catalog = CatalogKind.NONE;
          } else {
            throw bad("bad --catalog: " + value);
          }
          break;
        default:
          throw bad("unknown option: " + key);
      }
    }
    if (o.dt == null) {
      throw bad("--dt is required");
    }
    return o;
  }

  private static boolean hasControl(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (Character.isISOControl(s.charAt(i))) {
        return true;
      }
    }
    return false;
  }

  private static String date(String key, String value) {
    if (!DATE.matcher(value).matches()) {
      throw bad("bad " + key + " (expected YYYY-MM-DD): " + value);
    }
    return value;
  }

  private static IllegalArgumentException bad(String msg) {
    return new IllegalArgumentException(msg + "\n" + USAGE);
  }
}
