package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ModelSinksTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final Configuration conf = new Configuration();

  private static void writeSample(InodeModelSink s) throws Exception {
    s.inode(16385L, 'D', "");
    s.inode(16386L, 'D', "data");
    s.inode(16387L, 'F', "we\tird\nname\\x");
    s.inode(16388L, 'F', "orphan");
    s.block(1073741825L, 16387L);
    s.block(1073741826L, 16387L);
    s.block(-9223372036854775792L, 16388L);
    s.edge(16386L, 16385L);
    s.edge(16387L, 16386L);
    s.meta(42L, "fsimage_0000000000000000042", 1_762_430_000_000L);
    s.close();
  }

  private void roundTrip(MapperOptions.Format f, InodeModelSink sink, Path dir) throws Exception {
    writeSample(sink);
    ModelFiles m = ModelFiles.read(conf, dir, f);
    assertEquals("/data/we\tird\nname\\x", m.path(16387L));
    assertEquals("/data", m.path(16386L));
    assertEquals("/", m.path(16385L));
    assertNull(m.path(16388L));
    assertEquals(Character.valueOf('F'), m.types.get(16387L));
    assertEquals(Long.valueOf(16387L), m.blockInodes.get(1073741826L));
    assertEquals(Long.valueOf(16388L), m.blockInodes.get(-9223372036854775792L));
    assertEquals(42L, m.txid);
    assertEquals("fsimage_0000000000000000042", m.imageFile);
  }

  @Test
  public void orcRoundTrip() throws Exception {
    Path dir = new Path(tmp.getRoot().toURI().toString(), "out");
    roundTrip(MapperOptions.Format.ORC, new OrcModelSink(conf, dir), dir);
    assertTrue(new File(tmp.getRoot(), "out/inode/part-00000.orc").isFile());
    assertTrue(new File(tmp.getRoot(), "out/meta/part-00000.orc").isFile());
  }

  @Test
  public void orcFilesAreZlibCompressedVersion012() throws Exception {
    Path dir = new Path(tmp.getRoot().toURI().toString(), "out");
    writeSample(new OrcModelSink(conf, dir));
    org.apache.orc.Reader r = org.apache.orc.OrcFile.createReader(
        Tables.file(dir, Tables.BLOCK_INODE, "orc"), org.apache.orc.OrcFile.readerOptions(conf));
    assertEquals(org.apache.orc.CompressionKind.ZLIB, r.getCompressionKind());
    assertEquals(org.apache.orc.OrcFile.Version.V_0_12, r.getFileVersion());
    assertEquals("struct<block_id:bigint,inode_id:bigint>", r.getSchema().toString());
    assertEquals(3L, r.getNumberOfRows());
  }

  @Test
  public void tsvRoundTripEscapesNames() throws Exception {
    Path dir = new Path(tmp.getRoot().toURI().toString(), "out");
    FileSystem fs = dir.getFileSystem(conf);
    roundTrip(MapperOptions.Format.TSV, new TsvModelSink(fs, dir), dir);
    String inode = new String(java.nio.file.Files.readAllBytes(
        new File(tmp.getRoot(), "out/inode/part-00000.tsv").toPath()), "UTF-8");
    assertTrue(inode, inode.contains("16387\tF\twe\\tird\\nname\\\\x\n"));
  }

  @Test
  public void orcCrossesBatchBoundary() throws Exception {
    Path dir = new Path(tmp.getRoot().toURI().toString(), "out");
    OrcModelSink sink = new OrcModelSink(conf, dir);
    for (int i = 0; i < 2500; i++) {
      long id = 20000L + i;
      sink.inode(id, 'F', "file-" + i);
      sink.block(1_000_000L + i, id);
      sink.edge(id, ModelFiles.ROOT);
    }
    sink.meta(1L, "fsimage_1", 0L);
    sink.close();
    ModelFiles m = ModelFiles.read(conf, dir, MapperOptions.Format.ORC);
    assertEquals(2500, m.names.size());
    assertEquals("file-2499", m.names.get(20000L + 2499L));
    assertEquals(Long.valueOf(20000L + 1234), m.blockInodes.get(1_000_000L + 1234));
    assertEquals("/file-2499", m.path(20000L + 2499L));
  }

  @Test
  public void tsvFailedOpenThrowsCleanly() throws Exception {
    Path dir = new Path(tmp.getRoot().toURI().toString(), "out");
    File edgeDir = new File(tmp.getRoot(), "out/edge");
    assertTrue(edgeDir.mkdirs());
    assertTrue(new File(edgeDir, "part-00000.tsv").createNewFile());
    FileSystem fs = dir.getFileSystem(conf);
    try {
      new TsvModelSink(fs, dir);
      fail("expected the edge file to be rejected");
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("edge"));
    }
    Path other = new Path(tmp.getRoot().toURI().toString(), "other");
    roundTrip(MapperOptions.Format.TSV, new TsvModelSink(fs, other), other);
  }
}
