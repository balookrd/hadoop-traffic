package org.hadooptraffic.model;

/** One finished DataNode operation. Immutable. */
public final class TrafficEvent {
  private final String user;
  private final Op op;
  private final Role role;
  private final Locality locality;
  private final String blockPoolId;
  private final long blockId;
  private final long bytes;
  private final long estimatedBytes;
  private final long durationMs;
  private final String peer;

  public TrafficEvent(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, long bytes, long estimatedBytes, long durationMs, String peer) {
    this.user = user;
    this.op = op;
    this.role = role;
    this.locality = locality;
    this.blockPoolId = blockPoolId;
    this.blockId = blockId;
    this.bytes = bytes;
    this.estimatedBytes = estimatedBytes;
    this.durationMs = durationMs;
    this.peer = peer;
  }

  public String getUser() { return user; }
  public Op getOp() { return op; }
  public Role getRole() { return role; }
  public Locality getLocality() { return locality; }
  /** Block pool id, or null for WebHDFS HTTP-leg events. */
  public String getBlockPoolId() { return blockPoolId; }
  /** Block id, or -1 when not applicable. Kept for block-to-file mapping (step 2). */
  public long getBlockId() { return blockId; }
  public long getBytes() { return bytes; }
  public long getEstimatedBytes() { return estimatedBytes; }
  public long getDurationMs() { return durationMs; }
  public String getPeer() { return peer; }

  @Override
  public String toString() {
    return "TrafficEvent{user=" + user + ", op=" + op.label() + ", role=" + role.label()
        + ", locality=" + locality.label() + ", bpid=" + blockPoolId + ", blockId=" + blockId
        + ", bytes=" + bytes + ", estimatedBytes=" + estimatedBytes
        + ", durationMs=" + durationMs + ", peer=" + peer + '}';
  }
}
