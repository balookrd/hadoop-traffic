package org.hadooptraffic.advice;

import net.bytebuddy.asm.Advice;
import org.hadooptraffic.context.OpContexts;

/** BlockSender.sendPacket returns the payload length of the packet sent. */
public final class SendPacketAdvice {
  private SendPacketAdvice() {
  }

  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void exit(@Advice.Return int dataLen) {
    OpContexts.addBytes(dataLen);
  }
}
