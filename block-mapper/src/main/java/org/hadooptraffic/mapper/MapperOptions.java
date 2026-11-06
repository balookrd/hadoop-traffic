package org.hadooptraffic.mapper;

/** Command line of {@link FsImageMapper}. */
public final class MapperOptions {
  public static final String USAGE = "usage: hadoop jar block-mapper.jar "
      + "org.hadooptraffic.mapper.FsImageMapper -i <fsimage file> -o <output dir URI> "
      + "[--format orc|tsv]";

  /** Output format of the four tables. */
  public enum Format { ORC, TSV }

  private String image;
  private String output;
  private Format format = Format.ORC;

  private MapperOptions() {
  }

  public String image() { return image; }
  public String output() { return output; }
  public Format format() { return format; }

  public static MapperOptions parse(String[] args) {
    MapperOptions o = new MapperOptions();
    for (int i = 0; i < args.length; i++) {
      String key = args[i];
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("missing value for " + key + "\n" + USAGE);
      }
      String value = args[++i];
      switch (key) {
        case "-i":
          o.image = value;
          break;
        case "-o":
          o.output = value;
          break;
        case "--format":
          if ("orc".equals(value)) {
            o.format = Format.ORC;
          } else if ("tsv".equals(value)) {
            o.format = Format.TSV;
          } else {
            throw new IllegalArgumentException("unknown format: " + value + "\n" + USAGE);
          }
          break;
        default:
          throw new IllegalArgumentException("unknown option: " + key + "\n" + USAGE);
      }
    }
    if (o.image == null || o.output == null) {
      throw new IllegalArgumentException("-i and -o are required\n" + USAGE);
    }
    return o;
  }
}
