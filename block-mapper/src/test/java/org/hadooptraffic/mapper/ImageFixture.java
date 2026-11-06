package org.hadooptraffic.mapper;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;

/**
 * A namespace exercising every mapping rule, saved to an fsimage:
 * nested dirs, multi-block files, a directory rename (Hive staging pattern), a file renamed out
 * of a snapshotted directory (DstReference), a file deleted after a snapshot (snapshot-only
 * inode) and an erasure-coded file (block group ids).
 */
final class ImageFixture implements AutoCloseable {
  static final int BLOCK = 1024 * 1024;
  static final String EC_POLICY = "XOR-2-1-1024k";

  final MiniDFSCluster cluster;
  final File image;
  final Map<Long, String> livePaths = new HashMap<>();
  final Set<Long> snapshotOnlyBlocks = new HashSet<>();
  final long txid;

  private ImageFixture(MiniDFSCluster cluster, File image, long txid) {
    this.cluster = cluster;
    this.image = image;
    this.txid = txid;
  }

  static ImageFixture build() throws Exception {
    return build(false);
  }

  static ImageFixture build(boolean compressed) throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    if (compressed) {
      conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
      conf.set(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
          "org.apache.hadoop.io.compress.DefaultCodec");
    }
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    String[] live = {
        "/data/a/f1", "/data/a/f2", "/data/table/t1/part-0", "/data/moved-x", "/ec/e1"};
    DFSTestUtil.createFile(fs, new Path("/data/a/f1"), 2L * BLOCK + 12345, (short) 1, 1L);
    DFSTestUtil.createFile(fs, new Path("/data/a/f2"), 100, (short) 1, 2L);
    // Hive-style staging: write under a temporary directory, then rename the directory.
    DFSTestUtil.createFile(fs, new Path("/data/staging/t1/part-0"), BLOCK + 1, (short) 1, 3L);
    fs.mkdirs(new Path("/data/table"));
    fs.rename(new Path("/data/staging/t1"), new Path("/data/table/t1"));
    // Snapshot, then rename a file out of the snapshotted directory and delete another one.
    fs.mkdirs(new Path("/snap"));
    fs.allowSnapshot(new Path("/snap"));
    DFSTestUtil.createFile(fs, new Path("/snap/x"), 100, (short) 1, 4L);
    DFSTestUtil.createFile(fs, new Path("/snap/gone"), 100, (short) 1, 5L);
    fs.createSnapshot(new Path("/snap"), "s1");
    fs.rename(new Path("/snap/x"), new Path("/data/moved-x"));
    Set<Long> goneBlocks = new HashSet<>();
    for (LocatedBlock lb : fs.getClient().getLocatedBlocks("/snap/gone", 0, 100).getLocatedBlocks()) {
      goneBlocks.add(lb.getBlock().getBlockId());
    }
    fs.delete(new Path("/snap/gone"), false);
    // Erasure-coded file: the image stores block group ids.
    fs.enableErasureCodingPolicy(EC_POLICY);
    fs.mkdirs(new Path("/ec"));
    fs.setErasureCodingPolicy(new Path("/ec"), EC_POLICY);
    DFSTestUtil.createFile(fs, new Path("/ec/e1"), 3L * BLOCK, (short) 1, 6L);

    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    File image = FSImageTestUtil.findLatestImageFile(
        cluster.getNameNode().getFSImage().getStorage().getStorageDir(0));
    long txid = cluster.getNameNode().getFSImage().getStorage().getMostRecentCheckpointTxId();

    ImageFixture f = new ImageFixture(cluster, image, txid);
    for (String p : live) {
      long len = fs.getFileStatus(new Path(p)).getLen();
      for (LocatedBlock lb : fs.getClient().getLocatedBlocks(p, 0, len).getLocatedBlocks()) {
        f.livePaths.put(lb.getBlock().getBlockId(), p);
      }
    }
    f.snapshotOnlyBlocks.addAll(goneBlocks);
    return f;
  }

  @Override
  public void close() {
    cluster.shutdown();
  }
}
