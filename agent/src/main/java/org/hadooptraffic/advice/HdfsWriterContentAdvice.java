package org.hadooptraffic.advice;

import io.netty.handler.codec.http.HttpContent;
import net.bytebuddy.asm.Advice;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into HdfsWriter.channelRead0(ChannelHandlerContext, HttpContent). */
public final class HdfsWriterContentAdvice {
  private HdfsWriterContentAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void enter(@Advice.This Object self, @Advice.Argument(1) HttpContent chunk) {
    WebHdfsHooks.addBytes(self, chunk.content().readableBytes());
  }
}
