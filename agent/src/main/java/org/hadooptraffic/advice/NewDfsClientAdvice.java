package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.apache.hadoop.conf.Configuration;
import org.hadooptraffic.hooks.WebHdfsHooks;

/** Inlined into WebHdfsHandler.newDfsClient(String, Configuration): tags the client name. */
public final class NewDfsClientAdvice {
  private NewDfsClientAdvice() {
  }

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static void enter(@Advice.Argument(value = 1, readOnly = false) Configuration conf) {
    conf = WebHdfsHooks.tagClientConf(conf);
  }
}
