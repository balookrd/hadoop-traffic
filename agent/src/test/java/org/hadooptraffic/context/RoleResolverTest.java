package org.hadooptraffic.context;

import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.PIPELINE_SETUP_CREATE;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.TRANSFER_FINALIZED;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.TRANSFER_RBW;
import static org.junit.Assert.assertEquals;

import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;
import org.junit.Test;

public class RoleResolverTest {
  private static final String CLIENT = "DFSClient_NONMAPREDUCE_1_1";
  private static final String WEB = "DFSClient_WEBHDFS_1_1";

  @Test
  public void readByClient() {
    assertEquals(Role.CLIENT, RoleResolver.resolve(Op.READ, CLIENT, null, 0, 0));
  }

  @Test
  public void readByWebHdfsProxy() {
    assertEquals(Role.WEBHDFS_PROXY, RoleResolver.resolve(Op.READ, WEB, null, 0, 0));
  }

  @Test
  public void emptyClientNameIsDatanode() {
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.READ, "", null, 0, 0));
    assertEquals(Role.DATANODE,
        RoleResolver.resolve(Op.WRITE, "", PIPELINE_SETUP_CREATE, 0, 0));
  }

  @Test
  public void pipelineHeadIsClient() {
    assertEquals(Role.CLIENT,
        RoleResolver.resolve(Op.WRITE, CLIENT, PIPELINE_SETUP_CREATE, 3, 2));
  }

  @Test
  public void pipelineHeadForWebHdfsIsProxy() {
    assertEquals(Role.WEBHDFS_PROXY,
        RoleResolver.resolve(Op.WRITE, WEB, PIPELINE_SETUP_CREATE, 3, 2));
  }

  @Test
  public void downstreamIsPipelineEvenForWebHdfs() {
    assertEquals(Role.PIPELINE,
        RoleResolver.resolve(Op.WRITE, CLIENT, PIPELINE_SETUP_CREATE, 3, 1));
    assertEquals(Role.PIPELINE,
        RoleResolver.resolve(Op.WRITE, CLIENT, PIPELINE_SETUP_CREATE, 3, 0));
    assertEquals(Role.PIPELINE,
        RoleResolver.resolve(Op.WRITE, WEB, PIPELINE_SETUP_CREATE, 3, 0));
  }

  @Test
  public void transferStagesAreDatanode() {
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.WRITE, CLIENT, TRANSFER_RBW, 0, 0));
    assertEquals(Role.DATANODE,
        RoleResolver.resolve(Op.WRITE, CLIENT, TRANSFER_FINALIZED, 0, 1));
  }

  @Test
  public void balancerAndTransferOpsAreDatanode() {
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.COPY, null, null, 0, 0));
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.REPLACE, null, null, 0, 0));
    assertEquals(Role.DATANODE, RoleResolver.resolve(Op.TRANSFER, CLIENT, null, 0, 1));
  }

  @Test
  public void shortCircuitWithoutClientNameIsClient() {
    assertEquals(Role.CLIENT, RoleResolver.resolve(Op.SCR_FDS, null, null, 0, 0));
  }
}
