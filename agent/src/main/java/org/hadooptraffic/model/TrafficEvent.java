package org.hadooptraffic.model;

/**
 * One emission of a DataNode operation: an intermediate delta ({@code last == false}) or the
 * final remainder ({@code last == true}). Immutable.
 */
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
  private final long opSeq;
  private final long startMillis;
  private final long timestampMillis;
  private final boolean last;
  private final String path;

  public TrafficEvent(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, long bytes, long estimatedBytes, long durationMs, String peer,
      long opSeq, long startMillis, long timestampMillis, boolean last, String path) {
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
    this.opSeq = opSeq;
    this.startMillis = startMillis;
    this.timestampMillis = timestampMillis;
    this.last = last;
    this.path = path;
  }

  /**
   * A complete operation emitted now, without op id or path. Reads the clock once so that
   * {@code startMillis} and {@code timestampMillis} are consistent with {@code durationMs}.
   */
  public TrafficEvent(String user, Op op, Role role, Locality locality, String blockPoolId,
      long blockId, long bytes, long estimatedBytes, long durationMs, String peer) {
    long now = System.currentTimeMillis();
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
    this.opSeq = 0L;
    this.startMillis = now - durationMs;
    this.timestampMillis = now;
    this.last = true;
    this.path = null;
  }

  /**
   * Id of the block as the NameNode stores it: striped (negative) ids lose their index bits
   * and become the block group id; -1 (not applicable) and contiguous ids are unchanged.
   */
  public static long fileBlockId(long blockId) {
    return blockId < -1 ? blockId & ~0xFL : blockId;
  }

  public String getUser() { return user; }
  public Op getOp() { return op; }
  public Role getRole() { return role; }
  public Locality getLocality() { return locality; }
  /** Block pool id, or null for WebHDFS HTTP-leg events. */
  public String getBlockPoolId() { return blockPoolId; }
  /** Block id, or -1 when not applicable. */
  public long getBlockId() { return blockId; }
  public long getFileBlockId() { return fileBlockId(blockId); }
  /** Payload bytes since the previous event of the same operation. */
  public long getBytes() { return bytes; }
  public long getEstimatedBytes() { return estimatedBytes; }
  /** Time from operation start to this emission. */
  public long getDurationMs() { return durationMs; }
  public String getPeer() { return peer; }
  /** Groups the events of one operation; null for events built without one. */
  public String getOpId() { return opSeq == 0L ? null : OpIds.format(opSeq); }
  public long getStartMillis() { return startMillis; }
  public long getTimestampMillis() { return timestampMillis; }
  /** True for the final event of an operation (counted as one op). */
  public boolean isLast() { return last; }
  /** File path when the DataNode knows it (WebHDFS HTTP leg), otherwise null. */
  public String getPath() { return path; }

  @Override
  public String toString() {
    return "TrafficEvent{user=" + user + ", op=" + op.label() + ", role=" + role.label()
        + ", locality=" + locality.label() + ", bpid=" + blockPoolId + ", blockId=" + blockId
        + ", bytes=" + bytes + ", estimatedBytes=" + estimatedBytes
        + ", durationMs=" + durationMs + ", peer=" + peer + ", opId=" + getOpId()
        + ", last=" + last + ", path=" + path + '}';
  }
}
