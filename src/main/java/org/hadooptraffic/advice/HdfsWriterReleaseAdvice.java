package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into HdfsWriter.releaseDfsResources (end of body, channel close or error). */
public final class HdfsWriterReleaseAdvice {
  private HdfsWriterReleaseAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void enter(@Advice.This Object self) {
    WebHdfsHooks.finish(self);
  }
}
