package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** The reader on a synthetic image: every inode, block and edge, without MiniDFSCluster. */
public class SyntheticImageTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void readsEveryInodeBlockAndEdge() throws Exception {
    check(false);
  }

  @Test
  public void readsAnImageInHashOrder() throws Exception {
    check(true);
  }

  private void check(boolean hashOrder) throws Exception {
    SyntheticImage img = new SyntheticImage(3, 4, 5, 1000, 7);
    img.hashOrder = hashOrder;
    File f = new File(tmp.getRoot(), "fsimage_0000000000000000001");
    img.write(f);

    RecordingSink sink = new RecordingSink();
    new FsImageReader(new Configuration()).read(f, sink);

    assertEquals(img.inodes, sink.names.size());
    assertEquals(img.blocks, sink.blockInodes.size());
    // Every inode but the root has exactly one parent.
    assertEquals(img.inodes - 1, sink.parents.size());
    for (Map.Entry<Long, Long> e : sink.parents.entrySet()) {
      assertEquals("parent of " + e.getKey(), img.parentOf(e.getKey()), e.getValue().longValue());
    }
    assertEquals(SyntheticImage.TXID, sink.txid);
    assertEquals(1, sink.metaCalls);

    long firstFile = SyntheticImage.ROOT + img.dirs;
    assertEquals(Long.valueOf(firstFile), sink.blockInodes.get(SyntheticImage.FIRST_BLOCK));
    String path = sink.path(firstFile);
    assertNotNull(path);
    assertEquals(5, path.split("/").length); // "", db, table, partition, file
  }
}
