package org.hadooptraffic.it;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hadooptraffic.agent.TrafficAgent;

/** Reads the JSONL events the agent wrote in this JVM (active and closed files). */
public final class EventFiles {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private EventFiles() {
  }

  public static List<JsonNode> read() throws Exception {
    if (!TrafficAgent.eventSink().awaitIdle(10_000)) {
      throw new AssertionError("events sink did not drain in 10 s");
    }
    Path dir = TestAgent.eventsFile().getParent();
    List<JsonNode> out = new ArrayList<>();
    try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "events*.jsonl")) {
      for (Path p : ds) {
        List<String> lines;
        try {
          lines = Files.readAllLines(p, UTF_8);
        } catch (NoSuchFileException gone) {
          continue; // rotated while listing; its new name is read on the next poll
        }
        for (String line : lines) {
          if (!line.isEmpty()) {
            out.add(MAPPER.readTree(line));
          }
        }
      }
    }
    return out;
  }

  private static boolean matches(JsonNode e, String user, String op, String role) {
    return e.get("user").asText().equals(user) && e.get("op").asText().equals(op)
        && (role == null || e.get("role").asText().equals(role));
  }

  public static long bytes(List<JsonNode> events, String user, String op, String role) {
    long sum = 0;
    for (JsonNode e : events) {
      if (matches(e, user, op, role)) {
        sum += e.get("bytes").asLong();
      }
    }
    return sum;
  }

  public static long lastCount(List<JsonNode> events, String user, String op, String role) {
    long n = 0;
    for (JsonNode e : events) {
      if (matches(e, user, op, role) && e.get("last").asBoolean()) {
        n++;
      }
    }
    return n;
  }

  public static Set<String> paths(List<JsonNode> events, String user, String op) {
    Set<String> out = new HashSet<>();
    for (JsonNode e : events) {
      if (matches(e, user, op, null) && !e.get("path").isNull()) {
        out.add(e.get("path").asText());
      }
    }
    return out;
  }
}
