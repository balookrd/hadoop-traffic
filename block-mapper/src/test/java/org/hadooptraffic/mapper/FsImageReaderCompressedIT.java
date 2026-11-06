package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** The same namespace as {@link FsImageReaderIT}, saved with {@code dfs.image.compress=true}. */
public class FsImageReaderCompressedIT {
  private static ImageFixture fixture;
  private static RecordingSink sink;

  @BeforeClass
  public static void readImage() throws Exception {
    fixture = ImageFixture.build(true);
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
      assertNotNull("block " + e.getKey() + " missing", inode);
      assertEquals("block " + e.getKey(), e.getValue(), sink.path(inode));
    }
    assertEquals(fixture.txid, sink.txid);
  }
}
