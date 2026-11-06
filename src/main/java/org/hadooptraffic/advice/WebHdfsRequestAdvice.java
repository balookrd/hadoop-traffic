package org.hadooptraffic.advice;

import io.netty.channel.ChannelHandlerContext;
import net.bytebuddy.asm.Advice;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into WebHdfsHandler.onOpen/onCreate/onAppend. */
public final class WebHdfsRequestAdvice {
  private WebHdfsRequestAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static boolean enter(@Advice.Origin("#m") String method,
      @Advice.FieldValue("ugi") UserGroupInformation ugi,
      @Advice.Argument(0) ChannelHandlerContext ctx) {
    return WebHdfsHooks.onRequestEnter(method, ugi, ctx);
  }

  @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
  public static void exit(@Advice.Enter boolean started) {
    WebHdfsHooks.onRequestExit(started);
  }
}
