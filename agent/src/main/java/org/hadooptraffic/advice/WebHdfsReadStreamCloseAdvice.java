package org.hadooptraffic.advice;

import io.netty.handler.stream.ChunkedStream;
import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into close() of WebHdfsHandler's anonymous ChunkedStream. */
public final class WebHdfsReadStreamCloseAdvice {
  private WebHdfsReadStreamCloseAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void enter(@Advice.This ChunkedStream self) {
    WebHdfsHooks.finishRead(self);
  }
}
