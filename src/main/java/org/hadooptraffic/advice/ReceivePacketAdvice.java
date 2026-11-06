package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.context.OpContexts;

/** BlockReceiver.receivePacket returns payload length, 0 for heartbeats, -1 for the last packet. */
public final class ReceivePacketAdvice {
  private ReceivePacketAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(@Advice.Return int len) {
    OpContexts.addBytes(len);
  }
}
