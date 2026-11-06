package org.hadooptraffic.it;

import net.bytebuddy.agent.ByteBuddyAgent;
import org.hadooptraffic.agent.TrafficAgent;

/** Installs the agent into the test JVM (idempotent). */
public final class TestAgent {
  private TestAgent() {
  }

  public static void install() {
    TrafficAgent.install("maxUsers=100", ByteBuddyAgent.install());
  }
}
