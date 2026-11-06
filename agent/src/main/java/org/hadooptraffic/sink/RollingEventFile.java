package org.hadooptraffic.sink;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hadooptraffic.context.Clock;

/**
 * The active events file plus its closed, immutable siblings
 * {@code <base>-<yyyyMMdd-HHmmss>-<seq><ext>}, named after the UTC time of the file's first event.
 * A file never spans a UTC day of its events' {@code ts}; events reordered by a few
 * milliseconds across midnight stay in the earlier file. So the date in the name is the date
 * of every event in it.
 * Not thread-safe: used only by the sink's writer thread.
 */
final class RollingEventFile implements Closeable {
  private static final DateTimeFormatter STAMP =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
  private static final long DAY_MS = 86_400_000L;
  private static final Pattern TS = Pattern.compile("^\\{\"ts\":(\\d+)");

  private final Path active;
  private final Path dir;
  private final String base;
  private final String ext;
  private final Pattern closedPattern;
  private final long maxBytes;
  private final long maxAgeMillis;
  private final int maxFiles;
  private final Clock clock;
  private final AtomicLong filesDropped = new AtomicLong();

  private Writer out;
  private long size;
  // UTC time of the active file's first event; -1 when unknown (not opened by this instance).
  private long openedMillis = -1;
  // Wall-clock time the active file was opened; used only for age rotation.
  private long openedAtClock = -1;
  // UTC day number of the open file (openedMillis / DAY_MS); -1 when no file is open.
  private long fileDay = -1;
  private int seq;

  RollingEventFile(Path active, long maxBytes, long maxAgeMillis, int maxFiles, Clock clock) {
    this.active = active.toAbsolutePath();
    this.dir = this.active.getParent();
    String name = this.active.getFileName().toString();
    int dot = name.lastIndexOf('.');
    this.base = dot > 0 ? name.substring(0, dot) : name;
    this.ext = dot > 0 ? name.substring(dot) : "";
    this.closedPattern = Pattern.compile(
        Pattern.quote(base) + "-\\d{8}-\\d{6}-\\d+" + Pattern.quote(ext));
    this.maxBytes = maxBytes;
    this.maxAgeMillis = maxAgeMillis;
    this.maxFiles = maxFiles;
    this.clock = clock;
  }

  /**
   * Appends one line stamped {@code tsMillis}, opening the active file first if needed. An
   * event of a later UTC day closes the current file first; an earlier day never does.
   */
  void write(String line, long tsMillis) throws IOException {
    if (out != null && tsMillis / DAY_MS > fileDay) {
      rotate();
    }
    if (out == null) {
      open(tsMillis);
    }
    out.write(line);
    out.write('\n');
    size += line.length() + 1; // approximate: JSON is ASCII except user names and paths
  }

  void flush() throws IOException {
    if (out != null) {
      out.flush();
    }
  }

  /** Rotates when the active file is too big or too old (the day is decided per event). */
  void maybeRotate() throws IOException {
    if (out == null) {
      return;
    }
    long now = clock.currentTimeMillis();
    if (size >= maxBytes || now - openedAtClock >= maxAgeMillis) {
      rotate();
    }
  }

  /** Closes the active file and renames it to its closed name; drops the oldest extras. */
  void rotate() throws IOException {
    if (out != null) {
      out.close();
      out = null;
    }
    if (Files.exists(active) && Files.size(active) > 0) {
      // A file this instance did not open (crash leftover) is dated by its first event.
      long stamp = openedMillis >= 0 ? openedMillis : leftoverMillis();
      Files.move(active, closedName(stamp));
    }
    openedMillis = -1;
    openedAtClock = -1;
    fileDay = -1;
    size = 0;
    cleanup();
  }

  /** After an I/O error: forget the writer; the next write reopens (and rotates leftovers). */
  void abandon() {
    if (out != null) {
      try {
        out.close();
      } catch (IOException ignored) {
        // The file may be gone or the disk full; nothing more to do.
      }
      out = null;
    }
  }

  @Override
  public void close() throws IOException {
    rotate();
  }

  long filesDropped() {
    return filesDropped.get();
  }

  /** Closed files (names {@code <base>-<yyyyMMdd>-<HHmmss>-<seq><ext>}), oldest first. */
  List<Path> closedFiles() throws IOException {
    List<Path> files = new ArrayList<>();
    if (!Files.isDirectory(dir)) {
      return files;
    }
    try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir,
        p -> closedPattern.matcher(p.getFileName().toString()).matches())) {
      for (Path p : ds) {
        files.add(p);
      }
    }
    Collections.sort(files);
    return files;
  }

  private void open(long tsMillis) throws IOException {
    Files.createDirectories(dir);
    if (Files.exists(active) && Files.size(active) > 0) {
      // Left over from a crash or an earlier I/O error: close it out first.
      rotate();
    }
    out = Files.newBufferedWriter(active, UTF_8, StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
    openedMillis = tsMillis;
    openedAtClock = clock.currentTimeMillis();
    fileDay = tsMillis / DAY_MS;
    size = 0;
  }

  /** UTC time of a leftover file: ts of its first event, else its mtime. */
  private long leftoverMillis() throws IOException {
    try (BufferedReader r = Files.newBufferedReader(active, UTF_8)) {
      String first = r.readLine();
      if (first != null) {
        Matcher m = TS.matcher(first);
        if (m.find()) {
          return Long.parseLong(m.group(1));
        }
      }
    } catch (IOException | RuntimeException ignored) {
      // Unreadable or malformed: fall back to the modification time.
    }
    return Files.getLastModifiedTime(active).toMillis();
  }

  private Path closedName(long millis) {
    String stamp = STAMP.format(Instant.ofEpochMilli(millis));
    while (true) {
      Path p = dir.resolve(base + "-" + stamp + "-" + String.format("%06d", seq++) + ext);
      if (!Files.exists(p)) {
        return p;
      }
    }
  }

  /** Deletes the oldest closed files beyond maxFiles. Best effort: never blocks writing. */
  private void cleanup() {
    List<Path> closed;
    try {
      closed = closedFiles();
    } catch (IOException e) {
      return;
    }
    for (int i = 0; i < closed.size() - maxFiles; i++) {
      try {
        if (Files.deleteIfExists(closed.get(i))) {
          filesDropped.incrementAndGet();
        }
      } catch (IOException e) {
        // Undeletable (permissions, a directory): skip it, keep writing.
      }
    }
  }
}
