package org.hadooptraffic.hooks;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hadooptraffic.agent.AgentLog;

/** Reads WebHdfsHandler's private "path" field; any failure only disables path capture. */
final class WebHdfsPaths {
  private static final AtomicBoolean WARNED = new AtomicBoolean();
  private static volatile Lookup cached;

  private WebHdfsPaths() {
  }

  /** Field lookup result per handler class; field is null when the class has none. */
  private static final class Lookup {
    final Class<?> owner;
    final Field field;

    Lookup(Class<?> owner, Field field) {
      this.owner = owner;
      this.field = field;
    }
  }

  static String of(Object handler) {
    if (handler == null) {
      return null;
    }
    Lookup l = cached;
    if (l == null || l.owner != handler.getClass()) {
      l = new Lookup(handler.getClass(), find(handler.getClass()));
      cached = l;
    }
    if (l.field == null) {
      return null;
    }
    try {
      Object v = l.field.get(handler);
      return v instanceof String ? (String) v : null;
    } catch (Throwable t) {
      warnOnce(t);
      return null;
    }
  }

  private static Field find(Class<?> c) {
    try {
      Field f = c.getDeclaredField("path");
      f.setAccessible(true);
      return f;
    } catch (Throwable t) {
      if (c.getName().endsWith("WebHdfsHandler")) {
        warnOnce(t); // only the real handler is expected to have it
      }
      return null;
    }
  }

  private static void warnOnce(Throwable t) {
    if (WARNED.compareAndSet(false, true)) {
      AgentLog.warn("hdfs-traffic-agent: WebHDFS path capture is disabled", t);
    }
  }
}
