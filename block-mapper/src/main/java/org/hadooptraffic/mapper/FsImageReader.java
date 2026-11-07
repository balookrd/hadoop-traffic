package org.hadooptraffic.mapper;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Streams the sections of a protobuf fsimage that the inode model needs. Memory use is bounded
 * by the INODE_REFERENCE section (two values per reference); the tree is never built.
 *
 * <p>Decodes the wire format itself ({@link ProtoMessage}) instead of using Hadoop's generated
 * {@code FsImageProto}: those classes are built against protobuf 2.5 in Hadoop 3.1 and against
 * the shaded hadoop-thirdparty protobuf in 3.3, so bytecode compiled against one fails to link
 * on the other. The field numbers below come from {@code fsimage.proto} and {@code hdfs.proto}
 * and are the same in 3.1 and 3.3; {@code FsImageSchemaTest} checks them against the Hadoop
 * version the build runs with. Only public Hadoop API (configuration, codecs) is used.
 */
public final class FsImageReader {
  private static final String SUB_SUFFIX = "_SUB";

  // FSImageFormatProtobuf: the file starts with this magic and ends with a 4-byte big-endian
  // length of the delimited FileSummary that precedes it.
  static final byte[] MAGIC = "HDFSIMG1".getBytes(StandardCharsets.US_ASCII);
  static final int ON_DISK_VERSION = 1;
  private static final int SUMMARY_LENGTH_SIZE = 4;

  // FileSummary
  static final int SUMMARY_ON_DISK_VERSION = 1;
  static final int SUMMARY_CODEC = 3;
  static final int SUMMARY_SECTIONS = 4;
  // FileSummary.Section
  static final int SECTION_NAME = 1;
  static final int SECTION_LENGTH = 2;
  static final int SECTION_OFFSET = 3;
  // NameSystemSection
  static final int NS_TRANSACTION_ID = 6;
  // INodeSection (the header message of the INODE section)
  static final int INODES_NUM_INODES = 2;
  // INodeSection.INode
  static final int INODE_TYPE = 1;
  static final int INODE_ID = 2;
  static final int INODE_NAME = 3;
  static final int INODE_FILE = 4;
  // INodeSection.INode.Type
  static final int TYPE_FILE = 1;
  static final int TYPE_DIRECTORY = 2;
  static final int TYPE_SYMLINK = 3;
  // INodeSection.INodeFile
  static final int FILE_BLOCKS = 6;
  // hdfs.proto BlockProto
  static final int BLOCK_ID = 1;
  // INodeReferenceSection.INodeReference
  static final int REF_REFERRED_ID = 1;
  static final int REF_NAME = 2;
  // INodeDirectorySection.DirEntry
  static final int DIR_PARENT = 1;
  static final int DIR_CHILDREN = 2;
  static final int DIR_REF_CHILDREN = 3;

  private final Configuration conf;

  public FsImageReader(Configuration conf) {
    this.conf = conf;
  }

  /** One INODE_REFERENCE entry: whether it is named (a snapshot view) and what it refers to. */
  private static final class Reference {
    final long referredId;
    final boolean named;

    Reference(long referredId, boolean named) {
      this.referredId = referredId;
      this.named = named;
    }
  }

  /** One FileSummary.Section entry. */
  private static final class Section {
    final String name;
    final long offset;
    final long length;

    Section(String name, long offset, long length) {
      this.name = name;
      this.offset = offset;
      this.length = length;
    }
  }

  public void read(File image, InodeModelSink sink) throws IOException {
    ProtoMessage summary = loadSummary(image);
    String codec = "";
    long onDiskVersion = -1;
    Map<String, Section> sections = new HashMap<>();
    boolean parallel = false;
    while (summary.next()) {
      switch (summary.field()) {
        case SUMMARY_ON_DISK_VERSION:
          onDiskVersion = summary.varint();
          break;
        case SUMMARY_CODEC:
          codec = summary.string();
          break;
        case SUMMARY_SECTIONS:
          Section s = parseSection(summary.message());
          if (s.name.endsWith(SUB_SUFFIX)) {
            parallel = true;
          } else if (!sections.containsKey(s.name)) {
            sections.put(s.name, s);
          }
          break;
        default:
          summary.skip();
      }
    }
    if (onDiskVersion != ON_DISK_VERSION) {
      throw new IOException("unsupported fsimage on-disk version " + onDiskVersion + ": " + image);
    }
    if (parallel && !codec.isEmpty()) {
      throw new IOException("compressed fsimage saved in parallel (" + codec + ") is not "
          + "supported; save the image with dfs.image.parallel.save=false or without "
          + "dfs.image.compress");
    }
    for (String required : new String[] {"NS_INFO", "INODE", "INODE_DIR"}) {
      if (!sections.containsKey(required)) {
        throw new IOException("fsimage has no " + required + " section: " + image);
      }
    }
    CompressionCodec decompressor = codecFor(codec);

    try (FileInputStream fin = new FileInputStream(image)) {
      ProtoMessage nsInfo = ProtoMessage.readDelimited(open(fin, sections.get("NS_INFO"),
          decompressor));
      if (nsInfo == null) {
        throw new IOException("empty NS_INFO section: " + image);
      }
      long txid = 0;
      while (nsInfo.next()) {
        if (nsInfo.field() == NS_TRANSACTION_ID) {
          txid = nsInfo.varint();
        } else {
          nsInfo.skip();
        }
      }
      List<Reference> refs = sections.containsKey("INODE_REFERENCE")
          ? readReferences(open(fin, sections.get("INODE_REFERENCE"), decompressor))
          : new ArrayList<Reference>();
      readInodes(open(fin, sections.get("INODE"), decompressor), sink);
      readDirectories(open(fin, sections.get("INODE_DIR"), decompressor), refs, sink);
      sink.meta(txid, image.getName(), System.currentTimeMillis());
    }
  }

  /** Checks the magic header and returns the FileSummary message from the end of the file. */
  private static ProtoMessage loadSummary(File image) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(image, "r")) {
      long fileLength = raf.length();
      byte[] magic = new byte[MAGIC.length];
      if (fileLength < MAGIC.length + SUMMARY_LENGTH_SIZE) {
        throw new IOException("not a protobuf fsimage: " + image);
      }
      raf.readFully(magic);
      if (!Arrays.equals(magic, MAGIC)) {
        throw new IOException("not a protobuf fsimage: " + image);
      }
      raf.seek(fileLength - SUMMARY_LENGTH_SIZE);
      int summaryLength = raf.readInt();
      if (summaryLength <= 0
          || summaryLength > fileLength - MAGIC.length - SUMMARY_LENGTH_SIZE) {
        throw new IOException("invalid fsimage summary length " + summaryLength + ": " + image);
      }
      byte[] bytes = new byte[summaryLength];
      raf.seek(fileLength - SUMMARY_LENGTH_SIZE - summaryLength);
      raf.readFully(bytes);
      ProtoMessage summary = ProtoMessage.readDelimited(new ByteArrayInputStream(bytes));
      if (summary == null) {
        throw new IOException("empty fsimage summary: " + image);
      }
      return summary;
    }
  }

  private static Section parseSection(ProtoMessage m) throws IOException {
    String name = "";
    long offset = 0;
    long length = 0;
    while (m.next()) {
      switch (m.field()) {
        case SECTION_NAME:
          name = m.string();
          break;
        case SECTION_LENGTH:
          length = m.varint();
          break;
        case SECTION_OFFSET:
          offset = m.varint();
          break;
        default:
          m.skip();
      }
    }
    return new Section(name, offset, length);
  }

  /** The codec named in the summary (a class name, as FSImageCompression stores it). */
  private CompressionCodec codecFor(String codec) throws IOException {
    if (codec.isEmpty()) {
      return null;
    }
    CompressionCodec c = new CompressionCodecFactory(conf).getCodecByClassName(codec);
    if (c == null) {
      throw new IOException("fsimage codec is not available: " + codec);
    }
    return c;
  }

  /** A bounded, decompressed view of one section. Not closed: closing would close fin. */
  private static InputStream open(FileInputStream fin, Section s, CompressionCodec codec)
      throws IOException {
    fin.getChannel().position(s.offset);
    InputStream bounded = new BufferedInputStream(new BoundedInputStream(fin, s.length), 1 << 16);
    return codec == null ? bounded : codec.createInputStream(bounded);
  }

  private static List<Reference> readReferences(InputStream in) throws IOException {
    List<Reference> refs = new ArrayList<>();
    for (ProtoMessage r = ProtoMessage.readDelimited(in); r != null;
        r = ProtoMessage.readDelimited(in)) {
      long referredId = 0;
      boolean named = false;
      while (r.next()) {
        switch (r.field()) {
          case REF_REFERRED_ID:
            referredId = r.varint();
            break;
          case REF_NAME:
            named = true;
            r.skip();
            break;
          default:
            r.skip();
        }
      }
      refs.add(new Reference(referredId, named));
    }
    return refs;
  }

  private static void readInodes(InputStream in, InodeModelSink sink) throws IOException {
    ProtoMessage header = ProtoMessage.readDelimited(in);
    if (header == null) {
      throw new IOException("empty INODE section");
    }
    long numInodes = 0;
    while (header.next()) {
      if (header.field() == INODES_NUM_INODES) {
        numInodes = header.varint();
      } else {
        header.skip();
      }
    }
    ProtoMessage.LongList blocks = new ProtoMessage.LongList();
    for (long i = 0; i < numInodes; i++) {
      ProtoMessage inode = ProtoMessage.readDelimited(in);
      if (inode == null) {
        throw new IOException("INODE section ended after " + i + " of " + numInodes + " inodes");
      }
      long id = 0;
      long type = 0;
      String name = "";
      blocks.clear();
      while (inode.next()) {
        switch (inode.field()) {
          case INODE_TYPE:
            type = inode.varint();
            break;
          case INODE_ID:
            id = inode.varint();
            break;
          case INODE_NAME:
            name = inode.string();
            break;
          case INODE_FILE:
            readBlockIds(inode.message(), blocks);
            break;
          default:
            inode.skip();
        }
      }
      sink.inode(id, typeOf(type), name);
      for (int b = 0; b < blocks.size(); b++) {
        sink.block(blocks.get(b), id);
      }
    }
  }

  private static void readBlockIds(ProtoMessage file, ProtoMessage.LongList out)
      throws IOException {
    while (file.next()) {
      if (file.field() != FILE_BLOCKS) {
        file.skip();
        continue;
      }
      ProtoMessage block = file.message();
      while (block.next()) {
        if (block.field() == BLOCK_ID) {
          out.add(block.varint());
        } else {
          block.skip();
        }
      }
    }
  }

  private static void readDirectories(InputStream in, List<Reference> refs, InodeModelSink sink)
      throws IOException {
    ProtoMessage.LongList children = new ProtoMessage.LongList();
    ProtoMessage.LongList refChildren = new ProtoMessage.LongList();
    for (ProtoMessage e = ProtoMessage.readDelimited(in); e != null;
        e = ProtoMessage.readDelimited(in)) {
      long parent = 0;
      children.clear();
      refChildren.clear();
      while (e.next()) {
        switch (e.field()) {
          case DIR_PARENT:
            parent = e.varint();
            break;
          case DIR_CHILDREN:
            e.repeatedVarint(children);
            break;
          case DIR_REF_CHILDREN:
            e.repeatedVarint(refChildren);
            break;
          default:
            e.skip();
        }
      }
      for (int i = 0; i < children.size(); i++) {
        sink.edge(children.get(i), parent);
      }
      for (int i = 0; i < refChildren.size(); i++) {
        // uint32 on the wire: the low 32 bits are the index.
        long refIndex = refChildren.get(i) & 0xffffffffL;
        if (refIndex >= refs.size()) {
          throw new IOException("INODE_DIR refers to missing reference " + refIndex);
        }
        Reference r = refs.get((int) refIndex);
        // The named (WithName) case is defensive: current-state INODE_DIR entries normally hold
        // only DstReferences, while WithName lives in snapshot diffs. The skip branch below is
        // therefore not exercised by the fixture.
        if (!r.named) {
          // DstReference: the current location of a file renamed under snapshots.
          sink.edge(r.referredId, parent);
        }
      }
    }
  }

  private static char typeOf(long t) throws IOException {
    switch ((int) t) {
      case TYPE_FILE:
        return 'F';
      case TYPE_DIRECTORY:
        return 'D';
      case TYPE_SYMLINK:
        return 'S';
      default:
        throw new IOException("unsupported inode type " + t);
    }
  }

  /** Reads at most {@code remaining} bytes of the underlying stream and never closes it. */
  private static final class BoundedInputStream extends FilterInputStream {
    private long remaining;

    BoundedInputStream(InputStream in, long limit) {
      super(in);
      this.remaining = limit;
    }

    @Override
    public int read() throws IOException {
      if (remaining <= 0) {
        return -1;
      }
      int b = in.read();
      if (b >= 0) {
        remaining--;
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (remaining <= 0) {
        return -1;
      }
      int n = in.read(b, off, (int) Math.min(len, remaining));
      if (n > 0) {
        remaining -= n;
      }
      return n;
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = in.skip(Math.min(n, remaining));
      remaining -= skipped;
      return skipped;
    }

    @Override
    public int available() throws IOException {
      return (int) Math.min(in.available(), remaining);
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public void close() {
      // The caller owns the underlying stream.
    }
  }
}
