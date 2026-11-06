package org.hadooptraffic.metrics;

import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Plain MetricsInfo. Deliberately not using Interns.info(): its cache is capped (~2000 names)
 * and logs overflow warnings with high-cardinality per-user names.
 */
final class MetricName implements MetricsInfo {
  private final String name;
  private final String description;

  MetricName(String name, String description) {
    this.name = name;
    this.description = description;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public String toString() {
    return name;
  }
}
