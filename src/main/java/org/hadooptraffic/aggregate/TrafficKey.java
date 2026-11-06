package org.hadooptraffic.aggregate;

import java.util.Objects;
import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/** Aggregation key: one time series per distinct value. */
public final class TrafficKey {
  private final String user;
  private final Op op;
  private final Role role;
  private final Locality locality;

  public TrafficKey(String user, Op op, Role role, Locality locality) {
    this.user = Objects.requireNonNull(user, "user");
    this.op = Objects.requireNonNull(op, "op");
    this.role = Objects.requireNonNull(role, "role");
    this.locality = Objects.requireNonNull(locality, "locality");
  }

  public String getUser() { return user; }
  public Op getOp() { return op; }
  public Role getRole() { return role; }
  public Locality getLocality() { return locality; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TrafficKey)) {
      return false;
    }
    TrafficKey k = (TrafficKey) o;
    return user.equals(k.user) && op == k.op && role == k.role && locality == k.locality;
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, op, role, locality);
  }

  @Override
  public String toString() {
    return user + "/" + op.label() + "/" + role.label() + "/" + locality.label();
  }
}
