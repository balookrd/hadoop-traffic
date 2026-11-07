package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection.DirEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeReferenceSection.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.junit.Test;

/**
 * The field numbers {@link FsImageReader} decodes, checked against the generated classes of the
 * Hadoop version under test. CI runs it once per supported Hadoop line.
 */
public class FsImageSchemaTest {
  @Test
  public void fieldNumbersMatchFsImageProto() {
    assertEquals(FileSummary.ONDISKVERSION_FIELD_NUMBER, FsImageReader.SUMMARY_ON_DISK_VERSION);
    assertEquals(FileSummary.CODEC_FIELD_NUMBER, FsImageReader.SUMMARY_CODEC);
    assertEquals(FileSummary.SECTIONS_FIELD_NUMBER, FsImageReader.SUMMARY_SECTIONS);
    assertEquals(FileSummary.Section.NAME_FIELD_NUMBER, FsImageReader.SECTION_NAME);
    assertEquals(FileSummary.Section.LENGTH_FIELD_NUMBER, FsImageReader.SECTION_LENGTH);
    assertEquals(FileSummary.Section.OFFSET_FIELD_NUMBER, FsImageReader.SECTION_OFFSET);
    assertEquals(FsImageProto.NameSystemSection.TRANSACTIONID_FIELD_NUMBER,
        FsImageReader.NS_TRANSACTION_ID);
    assertEquals(INodeSection.NUMINODES_FIELD_NUMBER, FsImageReader.INODES_NUM_INODES);
    assertEquals(INodeSection.INode.TYPE_FIELD_NUMBER, FsImageReader.INODE_TYPE);
    assertEquals(INodeSection.INode.ID_FIELD_NUMBER, FsImageReader.INODE_ID);
    assertEquals(INodeSection.INode.NAME_FIELD_NUMBER, FsImageReader.INODE_NAME);
    assertEquals(INodeSection.INode.FILE_FIELD_NUMBER, FsImageReader.INODE_FILE);
    assertEquals(INodeSection.INode.Type.FILE_VALUE, FsImageReader.TYPE_FILE);
    assertEquals(INodeSection.INode.Type.DIRECTORY_VALUE, FsImageReader.TYPE_DIRECTORY);
    assertEquals(INodeSection.INode.Type.SYMLINK_VALUE, FsImageReader.TYPE_SYMLINK);
    assertEquals(INodeSection.INodeFile.BLOCKS_FIELD_NUMBER, FsImageReader.FILE_BLOCKS);
    assertEquals(BlockProto.BLOCKID_FIELD_NUMBER, FsImageReader.BLOCK_ID);
    assertEquals(INodeReference.REFERREDID_FIELD_NUMBER, FsImageReader.REF_REFERRED_ID);
    assertEquals(INodeReference.NAME_FIELD_NUMBER, FsImageReader.REF_NAME);
    assertEquals(DirEntry.PARENT_FIELD_NUMBER, FsImageReader.DIR_PARENT);
    assertEquals(DirEntry.CHILDREN_FIELD_NUMBER, FsImageReader.DIR_CHILDREN);
    assertEquals(DirEntry.REFCHILDREN_FIELD_NUMBER, FsImageReader.DIR_REF_CHILDREN);
  }
}
