package org.hadooptraffic.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class ProtoMessageTest {
  /** Minimal protobuf encoder for building test messages. */
  private static final class Enc {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    Enc varint(long v) {
      while ((v & ~0x7fL) != 0) {
        out.write((int) ((v & 0x7f) | 0x80));
        v >>>= 7;
      }
      out.write((int) v);
      return this;
    }

    Enc tag(int field, int wireType) {
      return varint(((long) field << 3) | wireType);
    }

    Enc bytes(int field, byte[] b) {
      tag(field, ProtoMessage.LENGTH_DELIMITED).varint(b.length);
      out.write(b, 0, b.length);
      return this;
    }

    Enc raw(int... b) {
      for (int x : b) {
        out.write(x);
      }
      return this;
    }

    byte[] build() {
      return out.toByteArray();
    }

    byte[] delimited() {
      byte[] body = build();
      return new Enc().varint(body.length).raw(toInts(body)).build();
    }

    private static int[] toInts(byte[] b) {
      int[] r = new int[b.length];
      for (int i = 0; i < b.length; i++) {
        r[i] = b[i] & 0xff;
      }
      return r;
    }
  }

  @Test
  public void readsVarintsIncludingUint64Max() throws IOException {
    ProtoMessage m = new ProtoMessage(new Enc()
        .tag(1, 0).varint(300)
        .tag(2, 0).varint(-1L)
        .build());
    assertTrue(m.next());
    assertEquals(1, m.field());
    assertEquals(300, m.varint());
    assertTrue(m.next());
    assertEquals(2, m.field());
    assertEquals(-1L, m.varint());
    assertFalse(m.next());
  }

  @Test
  public void readsStringsAndNestedMessages() throws IOException {
    byte[] inner = new Enc().tag(1, 0).varint(7).build();
    ProtoMessage m = new ProtoMessage(new Enc()
        .bytes(3, "имя".getBytes(StandardCharsets.UTF_8))
        .bytes(4, inner)
        .tag(5, 0).varint(9)
        .build());
    assertTrue(m.next());
    assertEquals("имя", m.string());
    assertTrue(m.next());
    ProtoMessage nested = m.message();
    assertTrue(nested.next());
    assertEquals(7, nested.varint());
    assertFalse(nested.next());
    assertTrue(m.next());
    assertEquals(5, m.field());
    assertEquals(9, m.varint());
  }

  @Test
  public void acceptsPackedAndUnpackedRepeatedVarints() throws IOException {
    byte[] packed = new Enc().varint(1).varint(300).varint(1L << 40).build();
    ProtoMessage m = new ProtoMessage(new Enc()
        .bytes(2, packed)
        .tag(2, 0).varint(5)
        .build());
    ProtoMessage.LongList out = new ProtoMessage.LongList();
    while (m.next()) {
      m.repeatedVarint(out);
    }
    assertEquals(4, out.size());
    assertEquals(1, out.get(0));
    assertEquals(300, out.get(1));
    assertEquals(1L << 40, out.get(2));
    assertEquals(5, out.get(3));
  }

  @Test
  public void skipsEveryWireType() throws IOException {
    ProtoMessage m = new ProtoMessage(new Enc()
        .tag(10, 0).varint(123456789)
        .tag(11, 1).raw(1, 2, 3, 4, 5, 6, 7, 8)
        .bytes(12, new byte[] {9, 9, 9})
        .tag(13, 5).raw(1, 2, 3, 4)
        .tag(14, 3).tag(1, 0).varint(1).tag(14, 4)
        .tag(1, 0).varint(42)
        .build());
    while (m.next()) {
      if (m.field() == 1) {
        assertEquals(42, m.varint());
        assertFalse(m.next());
        return;
      }
      m.skip();
    }
    fail("field 1 not reached");
  }

  @Test
  public void readDelimitedReturnsNullAtCleanEnd() throws IOException {
    byte[] one = new Enc().tag(1, 0).varint(1).delimited();
    byte[] two = new Enc().tag(1, 0).varint(2).delimited();
    byte[] both = new byte[one.length + two.length];
    System.arraycopy(one, 0, both, 0, one.length);
    System.arraycopy(two, 0, both, one.length, two.length);
    ByteArrayInputStream in = new ByteArrayInputStream(both);
    for (long expected = 1; expected <= 2; expected++) {
      ProtoMessage m = ProtoMessage.readDelimited(in);
      assertTrue(m.next());
      assertEquals(expected, m.varint());
    }
    assertNull(ProtoMessage.readDelimited(in));
  }

  @Test(expected = EOFException.class)
  public void readDelimitedRejectsTruncatedMessage() throws IOException {
    ProtoMessage.readDelimited(new ByteArrayInputStream(new byte[] {5, 8, 1}));
  }

  @Test(expected = IOException.class)
  public void rejectsLengthOverrunningTheMessage() throws IOException {
    ProtoMessage m = new ProtoMessage(new Enc().tag(3, 2).varint(10).raw(1, 2).build());
    assertTrue(m.next());
    m.string();
  }

  @Test(expected = IOException.class)
  public void rejectsTruncatedVarint() throws IOException {
    ProtoMessage m = new ProtoMessage(new Enc().tag(1, 0).raw(0x80).build());
    assertTrue(m.next());
    m.varint();
  }

  @Test(expected = IOException.class)
  public void rejectsFieldNumberZero() throws IOException {
    new ProtoMessage(new byte[] {0, 1}).next();
  }
}
