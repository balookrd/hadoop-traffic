package org.hadooptraffic.it;

import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** 3-DN MiniDFSCluster with block tokens (simple auth) and the agent installed. */
public abstract class ClusterTestBase {
  protected static final int BLOCK_SIZE = 1024 * 1024;
  protected static final int FILE_LEN = 2 * BLOCK_SIZE + 12345;
  protected static final long AWAIT_MS = 20_000;
  protected static final Path DIR = new Path("/traffic");

  protected static Configuration conf;
  protected static MiniDFSCluster cluster;

  @BeforeClass
  public static void startCluster() throws Exception {
    TestAgent.install();
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean("dfs.webhdfs.enabled", true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    fs.mkdirs(DIR);
    fs.setPermission(DIR, new FsPermission((short) 0777));
  }

  @AfterClass
  public static void stopCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  protected static <T> T as(String user, PrivilegedExceptionAction<T> action) throws Exception {
    return UserGroupInformation.createRemoteUser(user).doAs(action);
  }
}
