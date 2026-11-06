package org.hadooptraffic.spark;

import static org.junit.Assert.assertEquals;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ImagesTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private void image(String name, boolean withMeta) throws Exception {
    File dir = new File(tmp.getRoot(), name + "/meta");
    dir.mkdirs();
    if (withMeta) {
      new File(dir, "part-00000.orc").createNewFile();
    }
  }

  @Test
  public void picksTheLatestCompleteImage() throws Exception {
    image("dt=2026-11-05", true);
    image("dt=2026-11-06", true);
    image("dt=2026-11-07", false);          // mapper still running or failed: no meta file
    image(".dt=2026-11-08._tmp", true);     // hidden temp directory of a running mapper
    image("junk", true);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    assertEquals("2026-11-06", Images.latest(fs, new Path(tmp.getRoot().toURI())));
  }

  @Test(expected = IllegalStateException.class)
  public void failsWithoutAnyImage() throws Exception {
    Images.latest(FileSystem.getLocal(new Configuration()), new Path(tmp.getRoot().toURI()));
  }
}
