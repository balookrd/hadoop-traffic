package org.hadooptraffic.model;

/** Whether the peer runs on the same host as the DataNode. */
public enum Locality {
  LOCAL("local"),
  REMOTE("remote");

  private final String label;

  Locality(String label) {
    this.label = label;
  }

  public String label() {
    return label;
  }
}
