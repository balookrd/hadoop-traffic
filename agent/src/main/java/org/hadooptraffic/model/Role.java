package org.hadooptraffic.model;

/** Who is on the other side of the transfer, from the DataNode's point of view. */
public enum Role {
  /** An HDFS client (or the first DN of a client write pipeline). */
  CLIENT("client"),
  /** A downstream DN in a client write pipeline (replica forwarding). */
  PIPELINE("pipeline"),
  /** DataNode-initiated traffic: re-replication, balancer, pipeline recovery. */
  DATANODE("datanode"),
  /** Internal leg of a WebHDFS request (DN-embedded DFSClient). */
  WEBHDFS_PROXY("webhdfs_proxy");

  private final String label;

  Role(String label) {
    this.label = label;
  }

  public String label() {
    return label;
  }
}
