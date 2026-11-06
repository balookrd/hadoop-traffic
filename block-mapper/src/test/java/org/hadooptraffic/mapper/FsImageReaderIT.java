package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FsImageReaderIT {
  private static ImageFixture fixture;
  private static RecordingSink sink;

  @BeforeClass
  public static void readImage() throws Exception {
    fixture = ImageFixture.build();
    sink = new RecordingSink();
    new FsImageReader(new Configuration()).read(fixture.image, sink);
  }

  @AfterClass
  public static void stop() {
    if (fixture != null) {
      fixture.close();
    }
  }

  @Test
  public void everyLiveBlockResolvesToItsCurrentPath() {
    assertTrue(fixture.livePaths.size() >= 7);
    for (Map.Entry<Long, String> e : fixture.livePaths.entrySet()) {
      Long inode = sink.blockInodes.get(e.getKey());
      assertTrue("block " + e.getKey() + " missing", inode != null);
      assertEquals("block " + e.getKey(), e.getValue(), sink.path(inode));
    }
  }

  @Test
  public void snapshotOnlyBlocksDoNotResolve() {
    assertTrue(!fixture.snapshotOnlyBlocks.isEmpty());
    for (long b : fixture.snapshotOnlyBlocks) {
      Long inode = sink.blockInodes.get(b);
      assertNotNull("snapshot-only inode missing for block " + b, inode);
      assertNull(sink.path(inode));
    }
  }

  @Test
  public void typesAndRoot() {
    assertEquals("", sink.names.get(ModelFiles.ROOT));
    assertEquals(Character.valueOf('D'), sink.types.get(ModelFiles.ROOT));
    long f1 = sink.blockInodes.get(fixture.livePaths.entrySet().stream()
        .filter(e -> e.getValue().equals("/data/a/f1")).findFirst().get().getKey());
    assertEquals(Character.valueOf('F'), sink.types.get(f1));
  }

  @Test
  public void metaCarriesTxidAndFileNameOnce() {
    assertEquals(1, sink.metaCalls);
    assertEquals(fixture.txid, sink.txid);
    assertEquals(fixture.image.getName(), sink.imageFile);
  }

  @Test(expected = IOException.class)
  public void rejectsAFileThatIsNotAnImage() throws Exception {
    File junk = File.createTempFile("not-an-image", ".bin");
    junk.deleteOnExit();
    Files.write(junk.toPath(), new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    new FsImageReader(new Configuration()).read(junk, new RecordingSink());
  }
}
