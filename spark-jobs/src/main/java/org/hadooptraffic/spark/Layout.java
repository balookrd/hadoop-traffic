package org.hadooptraffic.spark;

/** Directories under the project root; one place for every path the jobs touch. */
public final class Layout {
  private final String root;

  public Layout(String root) {
    this.root = root;
  }

  public String raw(String dt) { return root + "/raw/dt=" + dt; }
  public String events(String dt) { return root + "/events/dt=" + dt; }
  public String fsimageRoot() { return root + "/fsimage"; }
  public String fsimage(String imageDt) { return fsimageRoot() + "/dt=" + imageDt; }
  public String fsimageTable(String imageDt, String table) {
    return fsimage(imageDt) + "/" + table;
  }
  public String eventPath(String dt) { return root + "/event_path/dt=" + dt; }
  public String trafficMinute(String dt) { return root + "/traffic_minute/dt=" + dt; }
}
