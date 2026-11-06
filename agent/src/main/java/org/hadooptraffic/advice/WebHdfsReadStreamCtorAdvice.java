package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into the constructor of WebHdfsHandler's anonymous ChunkedStream. */
public final class WebHdfsReadStreamCtorAdvice {
  private WebHdfsReadStreamCtorAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(@Advice.This Object self) {
    WebHdfsHooks.bindPending(self);
  }
}
