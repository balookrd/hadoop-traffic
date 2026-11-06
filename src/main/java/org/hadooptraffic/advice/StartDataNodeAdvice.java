package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.metrics.MetricsRegistrar;

/** Registers the metrics source once the DataNode (and its metrics system) has started. */
public final class StartDataNodeAdvice {
  private StartDataNodeAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit() {
    MetricsRegistrar.registerOnce();
  }
}
