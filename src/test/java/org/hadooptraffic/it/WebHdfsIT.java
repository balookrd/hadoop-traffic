package org.hadooptraffic.it;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.net.NetUtils;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.Test;

public class WebHdfsIT extends ClusterTestBase {

  private static URI webUri() throws Exception {
    return new URI("webhdfs://"
        + NetUtils.getHostPortString(cluster.getNameNode().getHttpAddress()));
  }

  @Test
  public void writeIsCountedOnHttpAndInternalLegs() throws Exception {
    String user = "erin-web-write";
    Path p = new Path(DIR, "web-write");
    as(user, () -> {
      try (FileSystem web = FileSystem.newInstance(webUri(), conf)) {
        DFSTestUtil.createFile(web, p, FILE_LEN, (short) 3, 5L);
      }
      return null;
    });

    Await.until(() -> TrafficAssert.bytes(user, Op.WEBHDFS_CREATE, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(user, Op.WRITE, Role.WEBHDFS_PROXY) == FILE_LEN
        && TrafficAssert.bytes(user, Op.WRITE, Role.PIPELINE) == 2L * FILE_LEN, AWAIT_MS);
    assertEquals(0L, TrafficAssert.bytes(user, Op.WRITE, Role.CLIENT));
  }

  @Test
  public void readIsCountedOnHttpAndInternalLegs() throws Exception {
    Path p = new Path(DIR, "web-read");
    as("writer-for-web-read", () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 6L);
      }
      return null;
    });
    String user = "frank-web-read";
    as(user, () -> {
      try (FileSystem web = FileSystem.newInstance(webUri(), conf)) {
        DFSTestUtil.readFile(web, p);
      }
      return null;
    });

    Await.until(() -> TrafficAssert.bytes(user, Op.WEBHDFS_READ, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(user, Op.READ, Role.WEBHDFS_PROXY) == FILE_LEN, AWAIT_MS);
    assertEquals(0L, TrafficAssert.bytes(user, Op.READ, Role.CLIENT));
  }
}
