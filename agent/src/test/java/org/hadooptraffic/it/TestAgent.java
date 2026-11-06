package org.hadooptraffic.it;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.hadooptraffic.agent.TrafficAgent;

/** Installs the agent into the test JVM (idempotent), with JMX user metrics and JSONL events on. */
public final class TestAgent {
  private static final Path EVENTS = Paths.get("target", "it-events",
      ManagementFactory.getRuntimeMXBean().getName().replaceAll("[^A-Za-z0-9_.-]", "_"),
      "events.jsonl").toAbsolutePath();

  private TestAgent() {
  }

  public static Path eventsFile() {
    return EVENTS;
  }

  public static void install() {
    deleteStaleEvents();
    TrafficAgent.install("userMetrics=true,maxUsers=100,events=" + EVENTS + ",flushIntervalSec=1",
        ByteBuddyAgent.install());
  }

  private static void deleteStaleEvents() {
    if (TrafficAgent.eventSink() != null) {
      return; // already installed in this JVM: the files are ours
    }
    File[] files = EVENTS.getParent().toFile().listFiles();
    if (files != null) {
      for (File f : files) {
        f.delete();
      }
    }
  }
}
