package org.hadooptraffic.context;

import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/** Classifies a DataTransferProtocol operation by who initiated it. */
public final class RoleResolver {
  /** Client-name prefix of DFSClients created by the DN WebHDFS handler (see WebHdfsHooks). */
  public static final String WEBHDFS_CLIENT_PREFIX = "DFSClient_WEBHDFS_";

  private RoleResolver() {
  }

  /**
   * @param pipelineSize writeBlock pipelineSize argument (full pipeline length as sent by the
   *     client; 0 for DN-initiated transfers)
   * @param targetCount number of downstream targets received by this DN
   */
  public static Role resolve(Op op, String clientName, BlockConstructionStage stage,
      int pipelineSize, int targetCount) {
    switch (op) {
      case COPY:
      case REPLACE:
      case TRANSFER:
        return Role.DATANODE;
      default:
        break;
    }
    if (clientName != null && clientName.isEmpty()) {
      return Role.DATANODE;
    }
    boolean webhdfs = clientName != null && clientName.startsWith(WEBHDFS_CLIENT_PREFIX);
    if (op == Op.WRITE) {
      if (stage == BlockConstructionStage.TRANSFER_RBW
          || stage == BlockConstructionStage.TRANSFER_FINALIZED) {
        return Role.DATANODE;
      }
      // The sender strips itself from the target list, so position 0 is the pipeline head.
      int position = pipelineSize - 1 - targetCount;
      if (position > 0) {
        return Role.PIPELINE;
      }
    }
    return webhdfs ? Role.WEBHDFS_PROXY : Role.CLIENT;
  }
}
