package org.hadooptraffic.context;

/** Thread-bound context of the DataXceiver / DataTransfer operation running on this thread. */
public final class OpContexts {
  private static final ThreadLocal<OpContext> CURRENT = new ThreadLocal<>();

  private OpContexts() {
  }

  public static OpContext current() {
    return CURRENT.get();
  }

  /**
   * Binds ctx to this thread unless an operation is already running (nested calls, e.g.
   * DataXceiver.transferBlock -> DataTransfer.run, keep accounting to the outer op).
   *
   * @return true if the caller started the context and must call {@link #end(boolean)}
   */
  public static boolean begin(OpContext ctx) {
    if (CURRENT.get() != null) {
      return false;
    }
    CURRENT.set(ctx);
    return true;
  }

  /**
   * Binds ctx unconditionally, for top-level DataXceiver ops which never nest. A context
   * left over from an earlier op on this pooled thread (missed end) is finished first so its
   * bytes are still reported and the thread recovers.
   *
   * @return always true; the caller must call {@link #end(boolean)}
   */
  public static boolean beginTopLevel(OpContext ctx) {
    OpContext previous = CURRENT.get();
    CURRENT.set(ctx);
    if (previous != null && previous != ctx) {
      previous.finish();
    }
    return true;
  }

  /** Unbinds the context started by {@link #begin(OpContext)} and emits it. */
  public static void end(boolean started) {
    if (!started) {
      return;
    }
    OpContext ctx = CURRENT.get();
    CURRENT.remove();
    if (ctx != null) {
      ctx.finish();
    }
  }

  public static void addBytes(long n) {
    OpContext ctx = CURRENT.get();
    if (ctx != null) {
      ctx.addBytes(n);
    }
  }
}
