package org.hadooptraffic.mapper;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes an uncompressed protobuf fsimage with a Hive-warehouse-like tree, for benchmarks and
 * for reader tests that need more inodes than MiniDFSCluster can make. Field numbers are those
 * of fsimage.proto; inodes carry the usual extra fields (times, permission, replication) so that
 * the reader also pays for skipping them.
 *
 * <p>Tree: root / {@code dbs} databases / {@code tablesPerDb} tables / {@code partsPerTable}
 * partitions / files. Inode ids start after the root (16385), block ids at 1073741825; every
 * {@code multiBlockEvery}-th file has three blocks, the others one.
 *
 * <pre>
 * java -cp ... org.hadooptraffic.mapper.SyntheticImage /tmp/fsimage_0000000000000000001 20000000 [hash]
 * </pre>
 */
final class SyntheticImage {
  static final long ROOT = 16385L;
  static final long FIRST_BLOCK = 1073741825L;
  static final long TXID = 1L;

  final int dbs;
  final int tablesPerDb;
  final int partsPerTable;
  final long files;
  final int multiBlockEvery;
  /** Write the INODE section in INodeMap bucket order, as the NameNode does. */
  boolean hashOrder;

  /** Filled by write(): counts for assertions. */
  long inodes;
  long blocks;
  long dirs;

  SyntheticImage(int dbs, int tablesPerDb, int partsPerTable, long files, int multiBlockEvery) {
    this.dbs = dbs;
    this.tablesPerDb = tablesPerDb;
    this.partsPerTable = partsPerTable;
    this.files = files;
    this.multiBlockEvery = multiBlockEvery;
  }

  /** A tree sized for roughly {@code files} files. */
  static SyntheticImage ofFiles(long files) {
    return new SyntheticImage(20, 50, 100, files, 10);
  }

  public static void main(String[] args) throws IOException {
    File out = new File(args[0]);
    long files = Long.parseLong(args[1]);
    long t0 = System.nanoTime();
    SyntheticImage img = ofFiles(files);
    img.hashOrder = args.length > 2 && "hash".equals(args[2]);
    img.write(out);
    System.out.printf("wrote %s: %d inodes (%d dirs), %d blocks, %d MB in %.1f s%n", out,
        img.inodes, img.dirs, img.blocks, out.length() >> 20, (System.nanoTime() - t0) / 1e9);
  }

  void write(File out) throws IOException {
    long leafDirs = (long) dbs * tablesPerDb * partsPerTable;
    long filesPerLeaf = Math.max(1, (files + leafDirs - 1) / leafDirs);
    dirs = 1 + dbs + (long) dbs * tablesPerDb + leafDirs;
    long fileCount = Math.min(files, leafDirs * filesPerLeaf);
    inodes = dirs + fileCount;
    blocks = fileCount + 2 * (multiBlockEvery > 0
        ? (fileCount + multiBlockEvery - 1) / multiBlockEvery : 0);

    try (CountingStream cs = new CountingStream(
        new BufferedOutputStream(new FileOutputStream(out), 1 << 20))) {
      cs.write("HDFSIMG1".getBytes(StandardCharsets.US_ASCII));
      List<long[]> sections = new ArrayList<>();
      List<String> names = new ArrayList<>();

      long start = cs.count;
      writeDelimited(cs, new Enc().varint(FsImageReader.NS_TRANSACTION_ID, TXID).bytes());
      names.add("NS_INFO");
      sections.add(new long[] {start, cs.count - start});

      start = cs.count;
      writeInodes(cs, filesPerLeaf, fileCount);
      names.add("INODE");
      sections.add(new long[] {start, cs.count - start});

      start = cs.count;
      writeDirs(cs, filesPerLeaf, fileCount);
      names.add("INODE_DIR");
      sections.add(new long[] {start, cs.count - start});

      Enc summary = new Enc()
          .varint(FsImageReader.SUMMARY_ON_DISK_VERSION, FsImageReader.ON_DISK_VERSION)
          .varint(2, -64 & 0xffffffffL); // layoutVersion, unused by the reader
      for (int i = 0; i < names.size(); i++) {
        summary.message(FsImageReader.SUMMARY_SECTIONS, new Enc()
            .string(FsImageReader.SECTION_NAME, names.get(i))
            .varint(FsImageReader.SECTION_LENGTH, sections.get(i)[1])
            .varint(FsImageReader.SECTION_OFFSET, sections.get(i)[0]));
      }
      ByteArrayOutputStream summaryBytes = new ByteArrayOutputStream();
      writeDelimited(summaryBytes, summary.bytes());
      summaryBytes.writeTo(cs);
      new DataOutputStream(cs).writeInt(summaryBytes.size());
    }
  }

  // Inode ids: root, dbs, tables, partitions (breadth-first), then files leaf by leaf.
  private long dbId(int d) {
    return ROOT + 1 + d;
  }

  private long tableId(int d, int t) {
    return ROOT + 1 + dbs + (long) d * tablesPerDb + t;
  }

  private long partId(long leaf) {
    return ROOT + 1 + dbs + (long) dbs * tablesPerDb + leaf;
  }

  private long fileId(long n) {
    return ROOT + dirs + n;
  }

  private void writeInodes(OutputStream out, long filesPerLeaf, long fileCount)
      throws IOException {
    writeDelimited(out, new Enc()
        .varint(1, ROOT + inodes - 1) // lastInodeId
        .varint(FsImageReader.INODES_NUM_INODES, inodes).bytes());
    if (hashOrder) {
      // INodeMap is a hash set indexed by id & (capacity - 1); the NameNode saves inodes in
      // bucket order, so consecutive ids land far apart in the INODE section.
      long capacity = Long.highestOneBit(Math.max(2, inodes / 4));
      long last = ROOT + inodes - 1;
      for (long bucket = 0; bucket < capacity; bucket++) {
        long id = (ROOT & ~(capacity - 1)) + bucket;
        for (; id <= last; id += capacity) {
          if (id >= ROOT) {
            writeDelimited(out, inode(id, filesPerLeaf, fileCount));
          }
        }
      }
    } else {
      for (long id = ROOT; id < ROOT + inodes; id++) {
        writeDelimited(out, inode(id, filesPerLeaf, fileCount));
      }
    }
  }

  private byte[] inode(long id, long filesPerLeaf, long fileCount) {
    if (id == ROOT) {
      return dir(ROOT, "");
    }
    if (id >= ROOT + dirs) {
      return file(id - ROOT - dirs, filesPerLeaf);
    }
    long k = id - ROOT - 1;
    if (k < dbs) {
      return dir(id, "db_" + k + ".db");
    }
    k -= dbs;
    if (k < (long) dbs * tablesPerDb) {
      return dir(id, "table_" + (k % tablesPerDb));
    }
    long leaf = k - (long) dbs * tablesPerDb;
    return dir(id, "dt=2026-" + (leaf % 12 + 1) + "-" + (leaf % 28 + 1) + "-" + leaf);
  }

  private byte[] file(long n, long filesPerLeaf) {
    boolean multi = multiBlockEvery > 0 && n % multiBlockEvery == 0;
    long multiBefore = multiBlockEvery > 0 ? (n + multiBlockEvery - 1) / multiBlockEvery : 0;
    long block = FIRST_BLOCK + n + 2 * multiBefore;
    Enc file = new Enc()
        .varint(1, 3) // replication
        .varint(2, 1793960100000L + n) // modificationTime
        .varint(3, 1793960100000L + n) // accessTime
        .varint(4, 134217728L) // preferredBlockSize
        .fixed64(5, 0x1ed00000001L); // permission
    int nBlocks = multi ? 3 : 1;
    for (int b = 0; b < nBlocks; b++) {
      file.message(FsImageReader.FILE_BLOCKS, new Enc()
          .varint(FsImageReader.BLOCK_ID, block + b)
          .varint(2, 1001 + n) // genStamp
          .varint(3, 134217728L)); // numBytes
    }
    long h = mix(n);
    String name = String.format("part-%05d-%08x-%04x.c000.zlib.orc", n % filesPerLeaf,
        (int) h, (int) (h >>> 40) & 0xffff);
    return new Enc()
        .varint(FsImageReader.INODE_TYPE, FsImageReader.TYPE_FILE)
        .varint(FsImageReader.INODE_ID, ROOT + dirs + n)
        .string(FsImageReader.INODE_NAME, name)
        .message(FsImageReader.INODE_FILE, file).bytes();
  }

  /** SplitMix64 finalizer: a fixed pseudo-random value per file. */
  private static long mix(long z) {
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return z ^ (z >>> 31);
  }

  private static byte[] dir(long id, String name) {
    return new Enc()
        .varint(FsImageReader.INODE_TYPE, FsImageReader.TYPE_DIRECTORY)
        .varint(FsImageReader.INODE_ID, id)
        .string(FsImageReader.INODE_NAME, name)
        .message(5, new Enc() // directory
            .varint(1, 1793960100000L) // modificationTime
            .varint(2, -1L) // nsQuota
            .varint(3, -1L) // dsQuota
            .fixed64(4, 0x1ed00000001L)) // permission
        .bytes();
  }

  private void writeDirs(OutputStream out, long filesPerLeaf, long fileCount)
      throws IOException {
    Enc root = new Enc().varint(FsImageReader.DIR_PARENT, ROOT);
    Enc kids = new Enc();
    for (int d = 0; d < dbs; d++) {
      kids.raw(dbId(d));
    }
    writeDelimited(out, root.message(FsImageReader.DIR_CHILDREN, kids).bytes());
    for (int d = 0; d < dbs; d++) {
      Enc tables = new Enc();
      for (int t = 0; t < tablesPerDb; t++) {
        tables.raw(tableId(d, t));
      }
      writeDelimited(out, new Enc().varint(FsImageReader.DIR_PARENT, dbId(d))
          .message(FsImageReader.DIR_CHILDREN, tables).bytes());
    }
    for (int d = 0; d < dbs; d++) {
      for (int t = 0; t < tablesPerDb; t++) {
        Enc parts = new Enc();
        for (int p = 0; p < partsPerTable; p++) {
          parts.raw(partId(((long) d * tablesPerDb + t) * partsPerTable + p));
        }
        writeDelimited(out, new Enc().varint(FsImageReader.DIR_PARENT, tableId(d, t))
            .message(FsImageReader.DIR_CHILDREN, parts).bytes());
      }
    }
    long leafDirs = (long) dbs * tablesPerDb * partsPerTable;
    for (long leaf = 0; leaf < leafDirs; leaf++) {
      long first = leaf * filesPerLeaf;
      if (first >= fileCount) {
        break;
      }
      Enc children = new Enc();
      for (long n = first; n < Math.min(fileCount, first + filesPerLeaf); n++) {
        children.raw(fileId(n));
      }
      writeDelimited(out, new Enc().varint(FsImageReader.DIR_PARENT, partId(leaf))
          .message(FsImageReader.DIR_CHILDREN, children).bytes());
    }
  }

  /** Parent of every non-root inode, for assertions. */
  long parentOf(long id) {
    long leafDirs = (long) dbs * tablesPerDb * partsPerTable;
    long filesPerLeaf = Math.max(1, (files + leafDirs - 1) / leafDirs);
    if (id >= ROOT + dirs) {
      return partId((id - ROOT - dirs) / filesPerLeaf);
    }
    long k = id - ROOT - 1;
    if (k < dbs) {
      return ROOT;
    }
    k -= dbs;
    if (k < (long) dbs * tablesPerDb) {
      return dbId((int) (k / tablesPerDb));
    }
    k -= (long) dbs * tablesPerDb;
    long table = k / partsPerTable;
    return tableId((int) (table / tablesPerDb), (int) (table % tablesPerDb));
  }

  static void writeDelimited(OutputStream out, byte[] msg) throws IOException {
    long len = msg.length;
    while ((len & ~0x7fL) != 0) {
      out.write((int) ((len & 0x7f) | 0x80));
      len >>>= 7;
    }
    out.write((int) len);
    out.write(msg);
  }

  /** Minimal protobuf encoder. */
  static final class Enc {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream(64);

    Enc raw(long v) {
      while ((v & ~0x7fL) != 0) {
        out.write((int) ((v & 0x7f) | 0x80));
        v >>>= 7;
      }
      out.write((int) v);
      return this;
    }

    private Enc tag(int field, int wireType) {
      return raw(((long) field << 3) | wireType);
    }

    Enc varint(int field, long v) {
      return tag(field, ProtoMessage.VARINT).raw(v);
    }

    Enc fixed64(int field, long v) {
      tag(field, ProtoMessage.FIXED64);
      for (int i = 0; i < 8; i++) {
        out.write((int) (v >>> (8 * i)));
      }
      return this;
    }

    Enc string(int field, String s) {
      byte[] b = s.getBytes(StandardCharsets.UTF_8);
      tag(field, ProtoMessage.LENGTH_DELIMITED).raw(b.length);
      out.write(b, 0, b.length);
      return this;
    }

    Enc message(int field, Enc m) {
      byte[] b = m.bytes();
      tag(field, ProtoMessage.LENGTH_DELIMITED).raw(b.length);
      out.write(b, 0, b.length);
      return this;
    }

    byte[] bytes() {
      return out.toByteArray();
    }
  }

  private static final class CountingStream extends java.io.FilterOutputStream {
    long count;

    CountingStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      count += len;
    }
  }
}
