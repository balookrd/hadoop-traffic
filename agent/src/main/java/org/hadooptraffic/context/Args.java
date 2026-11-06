package org.hadooptraffic.context;

/** Locates intercepted method arguments by type, tolerating signature drift across versions. */
public final class Args {
  private Args() {
  }

  public static <T> T first(Object[] args, Class<T> type) {
    if (args == null) {
      return null;
    }
    for (Object a : args) {
      if (type.isInstance(a)) {
        return type.cast(a);
      }
    }
    return null;
  }
}
