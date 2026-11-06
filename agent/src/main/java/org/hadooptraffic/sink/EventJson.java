package org.hadooptraffic.sink;

import org.hadooptraffic.model.TrafficEvent;

/** Formats one event as a single JSON line (no trailing newline). No JSON library needed. */
public final class EventJson {
  private EventJson() {
  }

  public static String format(TrafficEvent e, String dn) {
    StringBuilder sb = new StringBuilder(384);
    sb.append("{\"ts\":").append(e.getTimestampMillis());
    num(sb, "start", e.getStartMillis());
    str(sb, "dn", dn);
    str(sb, "op_id", e.getOpId());
    str(sb, "user", e.getUser());
    str(sb, "op", e.getOp().label());
    str(sb, "role", e.getRole().label());
    str(sb, "locality", e.getLocality().label());
    str(sb, "bpid", e.getBlockPoolId());
    num(sb, "block_id", e.getBlockId());
    num(sb, "file_block_id", e.getFileBlockId());
    num(sb, "bytes", e.getBytes());
    num(sb, "est_bytes", e.getEstimatedBytes());
    num(sb, "dur_ms", e.getDurationMs());
    sb.append(",\"last\":").append(e.isLast());
    str(sb, "peer", e.getPeer());
    str(sb, "path", e.getPath());
    return sb.append('}').toString();
  }

  private static void num(StringBuilder sb, String key, long v) {
    sb.append(",\"").append(key).append("\":").append(v);
  }

  private static void str(StringBuilder sb, String key, String v) {
    sb.append(",\"").append(key).append("\":");
    if (v == null) {
      sb.append("null");
      return;
    }
    sb.append('"');
    for (int i = 0; i < v.length(); i++) {
      char c = v.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    sb.append('"');
  }
}
