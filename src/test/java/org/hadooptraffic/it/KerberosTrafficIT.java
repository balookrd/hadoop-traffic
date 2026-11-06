package org.hadooptraffic.it;

import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.BeforeClass;
import org.junit.Test;

/** Kerberos + SASL data transfer + block tokens: the user still comes from the block token. */
public class KerberosTrafficIT extends SaslDataTransferTestCase {
  private static final int LEN = 1024 * 1024 + 123;

  @BeforeClass
  public static void installAgent() {
    TestAgent.install();
  }

  @Test
  public void kerberosUserIsTakenFromBlockToken() throws Exception {
    HdfsConfiguration conf = createSecureConfig("authentication");
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          getUserPrincipal(), getUserKeyTab());
      String user = ugi.getShortUserName();
      Path p = new Path("/kerberos-file");
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
          DFSTestUtil.createFile(fs, p, LEN, (short) 3, 9L);
          DFSTestUtil.readFile(fs, p);
        }
        return null;
      });

      Await.until(() -> TrafficAssert.bytes(user, Op.WRITE, Role.CLIENT) == LEN
          && TrafficAssert.bytes(user, Op.WRITE, Role.PIPELINE) == 2L * LEN
          && TrafficAssert.bytes(user, Op.READ, Role.CLIENT) == LEN, 30_000);
    } finally {
      cluster.shutdown();
    }
  }
}
