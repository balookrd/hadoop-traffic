package org.hadooptraffic.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.Test;

public class DataTransferProtocolIT extends ClusterTestBase {

  @Test
  public void clientWriteAndPipelineForwardingAreSeparated() throws Exception {
    String user = "alice-write";
    Path p = new Path(DIR, "write-file");
    as(user, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 3, 1L);
      }
      return null;
    });

    Await.until(() -> TrafficAssert.bytes(user, Op.WRITE, Role.CLIENT) == FILE_LEN
        && TrafficAssert.bytes(user, Op.WRITE, Role.PIPELINE) == 2L * FILE_LEN, AWAIT_MS);
    assertEquals(3L, TrafficAssert.ops(user, Op.WRITE, Role.CLIENT));
  }

  @Test
  public void readIsAttributedToReader() throws Exception {
    Path p = new Path(DIR, "read-file");
    as("writer-for-read", () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 2L);
      }
      return null;
    });
    String reader = "bob-read";
    as(reader, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.readFile(fs, p);
      }
      return null;
    });

    Await.until(() -> TrafficAssert.bytes(reader, Op.READ, Role.CLIENT) == FILE_LEN, AWAIT_MS);
    assertEquals(0L, TrafficAssert.bytes(reader, Op.WRITE, Role.CLIENT));
  }

  @Test
  public void namenodeTriggeredReplicationIsDatanodeRole() throws Exception {
    String dnUser = UserGroupInformation.getCurrentUser().getShortUserName();
    long transferBefore = TrafficAssert.bytes(dnUser, Op.TRANSFER, Role.DATANODE);
    long writeBefore = TrafficAssert.bytes(dnUser, Op.WRITE, Role.DATANODE);
    Path p = new Path(DIR, "replicate-file");
    as("carol-repl", () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 3L);
      }
      return null;
    });

    FileSystem admin = cluster.getFileSystem();
    admin.setReplication(p, (short) 2);
    DFSTestUtil.waitReplication(admin, p, (short) 2);

    Await.until(() ->
        TrafficAssert.bytes(dnUser, Op.TRANSFER, Role.DATANODE) - transferBefore == FILE_LEN
        && TrafficAssert.bytes(dnUser, Op.WRITE, Role.DATANODE) - writeBefore == FILE_LEN,
        AWAIT_MS);
  }

  @Test
  public void countersArePublishedViaJmx() throws Exception {
    String user = "dave-jmx";
    Path p = new Path(DIR, "jmx-file");
    as(user, () -> {
      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf)) {
        DFSTestUtil.createFile(fs, p, FILE_LEN, (short) 1, 4L);
      }
      return null;
    });
    Await.until(() -> TrafficAssert.bytes(user, Op.WRITE, Role.CLIENT) == FILE_LEN, AWAIT_MS);

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> names = mbs.queryNames(new ObjectName("Hadoop:service=*,name=UserTraffic"),
        null);
    assertEquals(1, names.size());
    ObjectName name = names.iterator().next();
    String prefix = "Bytes.user." + user + ".op.write.role.client.locality.";
    String[] attr = new String[1];
    // The metrics2 JMX cache refreshes after its TTL (~10 s), so poll.
    Await.until(() -> {
      try {
        for (MBeanAttributeInfo a : mbs.getMBeanInfo(name).getAttributes()) {
          if (a.getName().startsWith(prefix)) {
            attr[0] = a.getName();
            return true;
          }
        }
        return false;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }, 30_000);
    assertNotNull(attr[0]);
    assertEquals((long) FILE_LEN, ((Number) mbs.getAttribute(name, attr[0])).longValue());

    // Ambari and HTTP scrapers read the DataNode's /jmx servlet, which drops some attribute
    // names that the MBeanServer itself accepts.
    URL url = new URL("http://localhost:" + cluster.getDataNodes().get(0).getInfoPort()
        + "/jmx?qry=" + name);
    String json;
    try (InputStream in = url.openStream()) {
      json = new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8);
    }
    assertTrue("attribute " + attr[0] + " missing from " + url + ": " + json,
        json.contains("\"" + attr[0] + "\""));
  }
}
