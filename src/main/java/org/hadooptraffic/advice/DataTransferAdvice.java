package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.hadooptraffic.context.OpContexts;
import org.hadooptraffic.hooks.XceiverHooks;

/** Inlined into DataNode$DataTransfer.run. */
public final class DataTransferAdvice {
  private DataTransferAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static boolean enter(@Advice.FieldValue("b") ExtendedBlock block,
      @Advice.FieldValue("targets") DatanodeInfo[] targets) {
    return XceiverHooks.onDataTransferEnter(block, targets);
  }

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(@Advice.Enter boolean started) {
    OpContexts.end(started);
  }
}
