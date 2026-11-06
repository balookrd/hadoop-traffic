package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.apache.hadoop.hdfs.net.Peer;
import org.hadooptraffic.context.OpContexts;
import org.hadooptraffic.hooks.XceiverHooks;

/** Inlined into DataXceiver ops. Keep logic in XceiverHooks. */
public final class XceiverOpAdvice {
  private XceiverOpAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static boolean enter(@Advice.Origin("#m") String method,
      @Advice.AllArguments Object[] args, @Advice.FieldValue("peer") Peer peer) {
    return XceiverHooks.onXceiverEnter(method, args, peer);
  }

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(@Advice.Enter boolean started) {
    OpContexts.end(started);
  }
}
