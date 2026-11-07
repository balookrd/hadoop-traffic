package org.hadooptraffic.mapper;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * A cursor over one encoded protobuf message: just enough of the wire format to read the fsimage
 * fields the mapper needs, with no protobuf runtime. Unknown fields are skipped, so fields added
 * by newer Hadoop versions do not break reading.
 *
 * <p>Usage: {@code while (m.next()) switch (m.field()) { case 1: m.varint(); ... default: m.skip(); }}
 */
final class ProtoMessage {
  static final int VARINT = 0;
  static final int FIXED64 = 1;
  static final int LENGTH_DELIMITED = 2;
  static final int START_GROUP = 3;
  static final int END_GROUP = 4;
  static final int FIXED32 = 5;

  /** Same cap as protobuf-java's default size limit; a larger length means a corrupt stream. */
  private static final int MAX_MESSAGE_SIZE = 64 << 20;

  private final byte[] buf;
  private int pos;
  private final int limit;
  private int tag;

  ProtoMessage(byte[] buf, int offset, int length) {
    this.buf = buf;
    this.pos = offset;
    this.limit = offset + length;
  }

  ProtoMessage(byte[] buf) {
    this(buf, 0, buf.length);
  }

  /**
   * Reads one varint-length-prefixed message, as written by protobuf {@code writeDelimitedTo}.
   *
   * @return the message, or null at a clean end of stream
   */
  static ProtoMessage readDelimited(InputStream in) throws IOException {
    int first = in.read();
    if (first < 0) {
      return null;
    }
    long len = first & 0x7f;
    int shift = 7;
    int b = first;
    while ((b & 0x80) != 0) {
      if (shift > 28) {
        throw new IOException("malformed message length");
      }
      b = in.read();
      if (b < 0) {
        throw new EOFException("stream ended inside a message length");
      }
      len |= (long) (b & 0x7f) << shift;
      shift += 7;
    }
    if (len > MAX_MESSAGE_SIZE) {
      throw new IOException("message length " + len + " exceeds " + MAX_MESSAGE_SIZE);
    }
    byte[] data = new byte[(int) len];
    int off = 0;
    while (off < data.length) {
      int n = in.read(data, off, data.length - off);
      if (n < 0) {
        throw new EOFException("stream ended after " + off + " of " + len + " message bytes");
      }
      off += n;
    }
    return new ProtoMessage(data);
  }

  /** Advances to the next field; false at the end of the message. */
  boolean next() throws IOException {
    if (pos >= limit) {
      return false;
    }
    long t = varint();
    if (t >>> 3 == 0 || t > Integer.MAX_VALUE) {
      throw new IOException("invalid field tag " + t);
    }
    tag = (int) t;
    return true;
  }

  int field() {
    return tag >>> 3;
  }

  int wireType() {
    return tag & 7;
  }

  /** Reads a varint value (uint32, uint64, int64, bool, enum). */
  long varint() throws IOException {
    long result = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      if (pos >= limit) {
        throw new IOException("message ended inside a varint");
      }
      byte b = buf[pos++];
      result |= (long) (b & 0x7f) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new IOException("malformed varint");
  }

  /** Reads a length-delimited field as a nested message, sharing the buffer. */
  ProtoMessage message() throws IOException {
    int len = length();
    ProtoMessage m = new ProtoMessage(buf, pos, len);
    pos += len;
    return m;
  }

  /** Reads a length-delimited field as UTF-8; malformed bytes become U+FFFD. */
  String string() throws IOException {
    int len = length();
    String s = new String(buf, pos, len, StandardCharsets.UTF_8);
    pos += len;
    return s;
  }

  /**
   * Reads one occurrence of a repeated varint field into {@code out}: a packed run (wire type 2)
   * or a single value (wire type 0). Parsers must accept both encodings.
   */
  void repeatedVarint(LongList out) throws IOException {
    if (wireType() == VARINT) {
      out.add(varint());
      return;
    }
    if (wireType() != LENGTH_DELIMITED) {
      throw new IOException("field " + field() + ": wire type " + wireType()
          + " is not a varint or a packed run");
    }
    ProtoMessage run = message();
    while (run.pos < run.limit) {
      out.add(run.varint());
    }
  }

  /** Skips the value of the current field. */
  void skip() throws IOException {
    switch (wireType()) {
      case VARINT:
        varint();
        break;
      case FIXED64:
        advance(8);
        break;
      case LENGTH_DELIMITED:
        advance(length());
        break;
      case FIXED32:
        advance(4);
        break;
      case START_GROUP:
        skipGroup(field());
        break;
      default:
        throw new IOException("field " + field() + ": unsupported wire type " + wireType());
    }
  }

  private void skipGroup(int groupField) throws IOException {
    while (next()) {
      if (wireType() == END_GROUP) {
        if (field() != groupField) {
          throw new IOException("group " + groupField + " closed by field " + field());
        }
        return;
      }
      skip();
    }
    throw new IOException("message ended inside group " + groupField);
  }

  /** Checks that the current field is length-delimited and returns its length. */
  private int length() throws IOException {
    if (wireType() != LENGTH_DELIMITED) {
      throw new IOException("field " + field() + ": expected wire type 2, got " + wireType());
    }
    long len = varint();
    if (len < 0 || len > limit - pos) {
      throw new IOException("field " + field() + ": length " + len + " overruns the message");
    }
    return (int) len;
  }

  private void advance(int n) throws IOException {
    if (n > limit - pos) {
      throw new IOException("field " + field() + " overruns the message");
    }
    pos += n;
  }

  /** A growable list of primitive longs for repeated varint fields. */
  static final class LongList {
    private long[] values = new long[16];
    private int size;

    void add(long v) {
      if (size == values.length) {
        long[] grown = new long[size * 2];
        System.arraycopy(values, 0, grown, 0, size);
        values = grown;
      }
      values[size++] = v;
    }

    int size() {
      return size;
    }

    long get(int i) {
      if (i >= size) {
        throw new IndexOutOfBoundsException(i + " >= " + size);
      }
      return values[i];
    }

    void clear() {
      size = 0;
    }
  }
}
