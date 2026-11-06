package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into the HdfsWriter constructor (created inside onCreate/onAppend). */
public final class HdfsWriterCtorAdvice {
  private HdfsWriterCtorAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(@Advice.This Object self) {
    WebHdfsHooks.bindPending(self);
  }
}
