package org.hadooptraffic.hooks;

import java.io.IOException;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.hadooptraffic.context.Args;
import org.hadooptraffic.context.LocalityResolver;
import org.hadooptraffic.context.OpContext;
import org.hadooptraffic.context.OpContexts;
import org.hadooptraffic.context.RoleResolver;
import org.hadooptraffic.context.UserResolver;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/** Logic behind the DataTransferProtocol advice; kept out of advice for testability. */
public final class XceiverHooks {
  private XceiverHooks() {
  }

  /** Entry of a DataXceiver op. @return true if a context was started. */
  public static boolean onXceiverEnter(String method, Object[] args, Peer peer) {
    Op op = Op.fromXceiverMethod(method);
    if (op == null) {
      return false;
    }
    Token<?> token = Args.first(args, Token.class);
    ExtendedBlock block = Args.first(args, ExtendedBlock.class);
    // clientName is the first String only for read/write/transfer (replaceBlock's is delHint).
    String clientName =
        op == Op.READ || op == Op.WRITE || op == Op.TRANSFER ? Args.first(args, String.class)
            : null;
    BlockConstructionStage stage = Args.first(args, BlockConstructionStage.class);
    DatanodeInfo[] targets = Args.first(args, DatanodeInfo[].class);
    Integer pipelineSize = op == Op.WRITE ? Args.first(args, Integer.class) : null;

    Role role = RoleResolver.resolve(op, clientName, stage,
        pipelineSize == null ? 0 : pipelineSize, targets == null ? 0 : targets.length);
    OpContext ctx = new OpContext(UserResolver.userOf(token), op, role,
        LocalityResolver.of(peer),
        block == null ? null : block.getBlockPoolId(),
        block == null ? -1L : block.getBlockId(),
        peer == null ? null : peer.getRemoteAddressString());
    if (op == Op.SCR_FDS && block != null) {
      // Short-circuit reads bypass the DN; block length is the best available estimate.
      ctx.addEstimatedBytes(block.getNumBytes());
    }
    return OpContexts.beginTopLevel(ctx);
  }

  /** Entry of DataNode$DataTransfer.run (DN-initiated replication / pipeline recovery). */
  public static boolean onDataTransferEnter(ExtendedBlock block, DatanodeInfo[] targets) {
    if (OpContexts.current() != null) {
      return false;
    }
    DatanodeInfo target = targets != null && targets.length > 0 ? targets[0] : null;
    Locality locality = target == null ? Locality.REMOTE : LocalityResolver.ofHost(target.getIpAddr());
    OpContext ctx = new OpContext(dataNodeUser(), Op.TRANSFER, Role.DATANODE, locality,
        block == null ? null : block.getBlockPoolId(),
        block == null ? -1L : block.getBlockId(),
        target == null ? null : target.getXferAddr());
    return OpContexts.begin(ctx);
  }

  /** DataTransfer generates its block token for the DN's own user, so we report that user. */
  private static String dataNodeUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      return UserResolver.UNKNOWN;
    }
  }
}
