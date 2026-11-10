package org.hadooptraffic.sink;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.hadooptraffic.testutil.FakeClock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RollingEventFileTest {
  // 2026-11-06T10:15:00Z
  private static final long T0 = 1_793_960_100_000L;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final FakeClock clock = new FakeClock(T0);

  private RollingEventFile file(Path active, long maxBytes, long maxAgeMs, int maxFiles) {
    return new RollingEventFile(active, maxBytes, maxAgeMs, maxFiles, clock);
  }

  @Test
  public void writesLinesToActiveFileAndCreatesDirectory() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("sub/dir/events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("{\"a\":1}", clock.currentTimeMillis());
    f.write("{\"a\":2}", clock.currentTimeMillis());
    f.flush();
    assertEquals(Arrays.asList("{\"a\":1}", "{\"a\":2}"), Files.readAllLines(active, UTF_8));
  }

  @Test
  public void closedFileIsNamedAfterUtcOpenTime() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("x", clock.currentTimeMillis());
    clock.advanceSec(42);
    f.rotate();
    assertFalse(Files.exists(active));
    List<Path> closed = f.closedFiles();
    assertEquals(1, closed.size());
    assertEquals("events-20261106-101500-000000.jsonl", closed.get(0).getFileName().toString());
    assertEquals(Collections.singletonList("x"), Files.readAllLines(closed.get(0), UTF_8));
  }

  @Test
  public void rotatesBySize() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 10, 300_000, 10);
    f.write("12345", clock.currentTimeMillis());
    f.maybeRotate();
    assertTrue(Files.exists(active));
    f.write("67890", clock.currentTimeMillis());
    f.maybeRotate();
    assertFalse(Files.exists(active));
    assertEquals(1, f.closedFiles().size());
  }

  @Test
  public void rotatesByAge() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("x", clock.currentTimeMillis());
    clock.advanceSec(299);
    f.maybeRotate();
    assertTrue(Files.exists(active));
    clock.advanceSec(1);
    f.maybeRotate();
    assertFalse(Files.exists(active));
  }

  // 2026-11-05T23:59:59Z
  private static final long BEFORE_MIDNIGHT = 1_793_923_199_000L;

  @Test
  public void eventAfterMidnightOpensNewFile() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("first", BEFORE_MIDNIGHT);
    f.write("second", BEFORE_MIDNIGHT + 2_000);
    f.flush();
    List<Path> closed = f.closedFiles();
    assertEquals(1, closed.size());
    assertEquals("events-20261105-235959-000000.jsonl", closed.get(0).getFileName().toString());
    assertEquals(Collections.singletonList("first"), Files.readAllLines(closed.get(0), UTF_8));
    assertEquals(Collections.singletonList("second"), Files.readAllLines(active, UTF_8));
  }

  @Test
  public void lateEventOfPreviousDayStaysInCurrentFile() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("a", BEFORE_MIDNIGHT + 2_000);
    f.write("b", BEFORE_MIDNIGHT + 999);
    f.flush();
    assertEquals(Arrays.asList("a", "b"), Files.readAllLines(active, UTF_8));
    assertEquals(0, f.closedFiles().size());
  }

  @Test
  public void fileIsNamedAfterItsFirstEvent() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("x", T0 - 5_000);
    f.rotate();
    assertEquals("events-20261106-101455-000000.jsonl",
        f.closedFiles().get(0).getFileName().toString());
  }

  @Test
  public void keepsAtMostMaxClosedFiles() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 2);
    for (int i = 0; i < 3; i++) {
      f.write("line" + i, clock.currentTimeMillis());
      f.rotate();
      clock.advanceSec(1);
    }
    List<Path> closed = f.closedFiles();
    assertEquals(2, closed.size());
    assertEquals(Collections.singletonList("line1"), Files.readAllLines(closed.get(0), UTF_8));
    assertEquals(1L, f.filesDropped());
  }

  @Test
  public void leftoverActiveFileIsRotatedOnOpen() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    String oldLine = "{\"ts\":" + T0 + ",\"x\":1}";
    Files.write(active, (oldLine + "\n").getBytes(UTF_8));
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("new", clock.currentTimeMillis());
    f.flush();
    assertEquals(Collections.singletonList("new"), Files.readAllLines(active, UTF_8));
    List<Path> closed = f.closedFiles();
    assertEquals(1, closed.size());
    assertEquals(Collections.singletonList(oldLine), Files.readAllLines(closed.get(0), UTF_8));
  }

  @Test
  public void closeRotatesActiveFile() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("x", clock.currentTimeMillis());
    f.close();
    assertFalse(Files.exists(active));
    assertEquals(1, f.closedFiles().size());
  }

  @Test
  public void sameSecondRotationsGetDistinctNames() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("a", clock.currentTimeMillis());
    f.rotate();
    f.write("b", clock.currentTimeMillis());
    f.rotate();
    assertEquals(2, f.closedFiles().size());
  }

  @Test
  public void leftoverIsDatedByItsFirstEventNotItsMtime() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    Files.write(active, ("{\"ts\":" + T0 + ",\"x\":1}\n").getBytes(UTF_8));
    Files.setLastModifiedTime(active, FileTime.fromMillis(T0 + 86_400_000L));
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.close();
    assertFalse(Files.exists(active));
    assertEquals("events-20261106-101500-000000.jsonl",
        f.closedFiles().get(0).getFileName().toString());
  }

  @Test
  public void leftoverWithoutJsonFirstLineIsDatedByItsMtime() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    Files.write(active, "old\n".getBytes(UTF_8));
    Files.setLastModifiedTime(active, FileTime.fromMillis(T0));
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.close();
    assertFalse(Files.exists(active));
    assertEquals("events-20261106-101500-000000.jsonl",
        f.closedFiles().get(0).getFileName().toString());
  }

  @Test
  public void backlogOfOldEventsDoesNotRotateEveryWrite() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("old", T0 - 600_000);
    f.maybeRotate();
    assertTrue(Files.exists(active));
    clock.advanceSec(300);
    f.maybeRotate();
    assertFalse(Files.exists(active));
  }

  @Test
  public void abandonedFileIsResumedAndKeepsItsOpenTime() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 3_600_000, 10);
    f.write("a", clock.currentTimeMillis());
    f.flush();
    f.abandon();
    clock.advanceSec(600);
    f.write("b", clock.currentTimeMillis());
    f.flush();
    assertEquals(0, f.closedFiles().size());
    assertEquals(Arrays.asList("a", "b"), Files.readAllLines(active, UTF_8));
    f.rotate();
    assertEquals("events-20261106-101500-000000.jsonl",
        f.closedFiles().get(0).getFileName().toString());
  }

  @Test
  public void repeatedWriteErrorsDoNotEvictClosedFiles() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 2);
    for (int i = 0; i < 2; i++) {
      f.write("full" + i, clock.currentTimeMillis());
      f.rotate();
      clock.advanceSec(1);
    }
    // A full disk: every batch gets partly written, then fails and is abandoned.
    for (int i = 0; i < 5; i++) {
      f.write("partial" + i, clock.currentTimeMillis());
      f.flush();
      f.abandon();
    }
    List<Path> closed = f.closedFiles();
    assertEquals(2, closed.size());
    assertEquals(Collections.singletonList("full0"), Files.readAllLines(closed.get(0), UTF_8));
    assertEquals(0L, f.filesDropped());
  }

  @Test
  public void tornLastLineIsTerminatedOnResume() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("a", clock.currentTimeMillis());
    f.flush();
    f.abandon();
    Files.write(active, "{\"torn".getBytes(UTF_8), java.nio.file.StandardOpenOption.APPEND);
    f.write("b", clock.currentTimeMillis());
    f.flush();
    assertEquals(Arrays.asList("a", "{\"torn", "b"), Files.readAllLines(active, UTF_8));
  }

  @Test
  public void resumedFileStillRotatesBySizeAndAge() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 10, 300_000, 10);
    f.write("12345", clock.currentTimeMillis());
    f.flush();
    f.abandon();
    f.write("67890", clock.currentTimeMillis());
    f.flush();
    f.maybeRotate(); // 12 bytes on disk >= 10
    assertFalse(Files.exists(active));
    assertEquals(1, f.closedFiles().size());

    f.write("x", clock.currentTimeMillis());
    f.flush();
    f.abandon();
    clock.advanceSec(300);
    f.write("y", clock.currentTimeMillis());
    f.flush();
    f.maybeRotate();
    assertFalse(Files.exists(active));
    assertEquals(2, f.closedFiles().size());
  }

  @Test
  public void abandonedFileIsClosedByAnEventOfTheNextDay() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("first", BEFORE_MIDNIGHT);
    f.flush();
    f.abandon();
    f.write("second", BEFORE_MIDNIGHT + 2_000);
    f.flush();
    List<Path> closed = f.closedFiles();
    assertEquals(1, closed.size());
    assertEquals(Collections.singletonList("first"), Files.readAllLines(closed.get(0), UTF_8));
    assertEquals(Collections.singletonList("second"), Files.readAllLines(active, UTF_8));
  }

  @Test
  public void abandonedFileThatDisappearedIsReplacedByANewOne() throws Exception {
    Path active = tmp.getRoot().toPath().resolve("events.jsonl");
    RollingEventFile f = file(active, 1 << 20, 300_000, 10);
    f.write("a", clock.currentTimeMillis());
    f.flush();
    f.abandon();
    Files.delete(active);
    clock.advanceSec(60);
    f.write("b", clock.currentTimeMillis());
    f.rotate();
    assertEquals("events-20261106-101600-000000.jsonl",
        f.closedFiles().get(0).getFileName().toString());
  }

  @Test
  public void foreignFilesAreNeitherCountedNorDeleted() throws Exception {
    Path dir = tmp.getRoot().toPath();
    Path foreign = dir.resolve("events-old.jsonl");
    Files.write(foreign, "keep\n".getBytes(UTF_8));
    RollingEventFile f = file(dir.resolve("events.jsonl"), 1 << 20, 300_000, 1);
    f.write("a", clock.currentTimeMillis());
    f.rotate();
    clock.advanceSec(1);
    f.write("b", clock.currentTimeMillis());
    f.rotate();
    assertTrue(Files.exists(foreign));
    assertEquals(1, f.closedFiles().size());
    assertEquals(1L, f.filesDropped());
  }

  @Test
  public void undeletableOldFileDoesNotBlockWriting() throws Exception {
    Path dir = tmp.getRoot().toPath();
    // A non-empty directory with a closed-file name cannot be deleted.
    Path stuck = dir.resolve("events-20200101-000000-000000.jsonl");
    Files.createDirectories(stuck);
    Files.write(stuck.resolve("x"), "x".getBytes(UTF_8));
    Path active = dir.resolve("events.jsonl");
    Files.write(active, "leftover\n".getBytes(UTF_8));
    RollingEventFile f = file(active, 1 << 20, 300_000, 1);
    f.write("new", clock.currentTimeMillis());
    f.flush();
    assertEquals(Collections.singletonList("new"), Files.readAllLines(active, UTF_8));
    assertTrue(Files.isDirectory(stuck));
    assertEquals(0L, f.filesDropped());
  }
}
