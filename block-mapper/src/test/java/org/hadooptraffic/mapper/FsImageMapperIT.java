package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FsImageMapperIT {
  private static ImageFixture fixture;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final Configuration conf = new Configuration();
  private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
  private final PrintStream err = new PrintStream(errBytes, true);

  @BeforeClass
  public static void buildImage() throws Exception {
    fixture = ImageFixture.build();
  }

  @AfterClass
  public static void stop() {
    if (fixture != null) {
      fixture.close();
    }
  }

  private String out(String name) {
    return new File(tmp.getRoot(), name).toURI().toString();
  }

  private void assertResolvesLiveBlocks(String out, MapperOptions.Format f) throws Exception {
    ModelFiles m = ModelFiles.read(conf, new Path(out), f);
    for (Map.Entry<Long, String> e : fixture.livePaths.entrySet()) {
      assertEquals("block " + e.getKey(), e.getValue(), m.path(m.blockInodes.get(e.getKey())));
    }
    assertEquals(fixture.txid, m.txid);
  }

  @Test
  public void writesOrcByDefault() throws Exception {
    String out = out("dt=2026-11-06");
    assertEquals(0, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", out}, conf, err));
    assertResolvesLiveBlocks(out, MapperOptions.Format.ORC);
    assertFalse(new File(tmp.getRoot(), ".dt=2026-11-06._tmp").exists());
  }

  @Test
  public void writesTsvOnRequest() throws Exception {
    String out = out("tsv");
    assertEquals(0, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", out, "--format", "tsv"}, conf, err));
    assertResolvesLiveBlocks(out, MapperOptions.Format.TSV);
  }

  @Test
  public void badImageFailsAndLeavesNothing() throws Exception {
    File junk = tmp.newFile("junk");
    Files.write(junk.toPath(), new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    String out = out("bad");
    assertEquals(1, FsImageMapper.run(new String[] {"-i", junk.getPath(), "-o", out}, conf, err));
    assertFalse(new File(tmp.getRoot(), "bad").exists());
    assertFalse(new File(tmp.getRoot(), ".bad._tmp").exists());
    assertTrue(errBytes.toString(), errBytes.toString().contains("block-mapper:"));
  }

  @Test
  public void existingOutputIsNotOverwritten() throws Exception {
    File existing = tmp.newFolder("exists");
    assertEquals(1, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", existing.toURI().toString()}, conf,
        err));
    assertEquals(0, existing.list().length);
  }

  @Test
  public void staleTempDirIsReplaced() throws Exception {
    File stale = tmp.newFolder(".again._tmp");
    Files.write(new File(stale, "junk").toPath(), new byte[] {1});
    String out = out("again");
    assertEquals(0, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", out}, conf, err));
    assertFalse(new File(tmp.getRoot(), "again/junk").exists());
  }

  @Test
  public void rootOutputIsAUsageError() {
    assertEquals(2, FsImageMapper.run(
        new String[] {"-i", fixture.image.getPath(), "-o", "/"}, conf, err));
    assertTrue(errBytes.toString(), errBytes.toString().contains("block-mapper:"));
  }

  @Test
  public void missingImageFailsBeforeWriting() {
    assertEquals(1, FsImageMapper.run(
        new String[] {"-i", new File(tmp.getRoot(), "nope").getPath(), "-o", out("miss")},
        conf, err));
    assertFalse(new File(tmp.getRoot(), "miss").exists());
    assertFalse(new File(tmp.getRoot(), ".miss._tmp").exists());
    assertTrue(errBytes.toString(),
        errBytes.toString().contains("block-mapper: fsimage not found: "));
  }

  @Test
  public void usageErrorReturnsTwo() {
    assertEquals(2, FsImageMapper.run(new String[] {"-i", "x"}, conf, err));
    assertTrue(errBytes.toString().contains("usage:"));
  }
}
