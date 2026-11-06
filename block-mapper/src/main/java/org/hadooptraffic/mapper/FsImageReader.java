package org.hadooptraffic.mapper;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.util.LimitInputStream;

/**
 * Streams the sections of a protobuf fsimage that the inode model needs. Memory use is bounded
 * by the INODE_REFERENCE section (two values per reference); the tree is never built.
 *
 * <p>Calls only methods of the generated FsImageProto classes and never names a protobuf runtime
 * type, so the same source compiles against Hadoop 3.1 (com.google.protobuf) and 3.3 (shaded).
 */
public final class FsImageReader {
  private static final String SUB_SUFFIX = "_SUB";

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

  public void read(File image, InodeModelSink sink) throws IOException {
    FsImageProto.FileSummary summary;
    try (RandomAccessFile raf = new RandomAccessFile(image, "r")) {
      if (!FSImageUtil.checkFileFormat(raf)) {
        throw new IOException("not a protobuf fsimage: " + image);
      }
      summary = FSImageUtil.loadSummary(raf);
    }
    String codec = summary.getCodec();
    Map<String, FsImageProto.FileSummary.Section> sections = new HashMap<>();
    boolean parallel = false;
    for (FsImageProto.FileSummary.Section s : summary.getSectionsList()) {
      if (s.getName().endsWith(SUB_SUFFIX)) {
        parallel = true;
      } else if (!sections.containsKey(s.getName())) {
        sections.put(s.getName(), s);
      }
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

    try (FileInputStream fin = new FileInputStream(image)) {
      FsImageProto.NameSystemSection nsInfo = FsImageProto.NameSystemSection
          .parseDelimitedFrom(open(fin, sections.get("NS_INFO"), codec));
      if (nsInfo == null) {
        throw new IOException("empty NS_INFO section: " + image);
      }
      long txid = nsInfo.getTransactionId();
      List<Reference> refs = sections.containsKey("INODE_REFERENCE")
          ? readReferences(open(fin, sections.get("INODE_REFERENCE"), codec))
          : new ArrayList<Reference>();
      readInodes(open(fin, sections.get("INODE"), codec), sink);
      readDirectories(open(fin, sections.get("INODE_DIR"), codec), refs, sink);
      sink.meta(txid, image.getName(), System.currentTimeMillis());
    }
  }

  /** A bounded, decompressed view of one section. Not closed: closing would close fin. */
  private InputStream open(FileInputStream fin, FsImageProto.FileSummary.Section s, String codec)
      throws IOException {
    fin.getChannel().position(s.getOffset());
    InputStream bounded = new BufferedInputStream(new LimitInputStream(fin, s.getLength()),
        1 << 16);
    return FSImageUtil.wrapInputStreamForCompression(conf, codec, bounded);
  }

  private static List<Reference> readReferences(InputStream in) throws IOException {
    List<Reference> refs = new ArrayList<>();
    for (FsImageProto.INodeReferenceSection.INodeReference r =
            FsImageProto.INodeReferenceSection.INodeReference.parseDelimitedFrom(in);
        r != null;
        r = FsImageProto.INodeReferenceSection.INodeReference.parseDelimitedFrom(in)) {
      refs.add(new Reference(r.getReferredId(), r.hasName()));
    }
    return refs;
  }

  private static void readInodes(InputStream in, InodeModelSink sink) throws IOException {
    FsImageProto.INodeSection header = FsImageProto.INodeSection.parseDelimitedFrom(in);
    if (header == null) {
      throw new IOException("empty INODE section");
    }
    for (long i = 0; i < header.getNumInodes(); i++) {
      FsImageProto.INodeSection.INode inode = FsImageProto.INodeSection.INode.parseDelimitedFrom(in);
      if (inode == null) {
        throw new IOException("INODE section ended after " + i + " of "
            + header.getNumInodes() + " inodes");
      }
      sink.inode(inode.getId(), typeOf(inode.getType()), inode.getName().toStringUtf8());
      if (inode.hasFile()) {
        for (HdfsProtos.BlockProto b : inode.getFile().getBlocksList()) {
          sink.block(b.getBlockId(), inode.getId());
        }
      }
    }
  }

  private static void readDirectories(InputStream in, List<Reference> refs, InodeModelSink sink)
      throws IOException {
    for (FsImageProto.INodeDirectorySection.DirEntry e =
            FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
        e != null;
        e = FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in)) {
      long parent = e.getParent();
      for (long child : e.getChildrenList()) {
        sink.edge(child, parent);
      }
      for (int refIndex : e.getRefChildrenList()) {
        if (refIndex < 0 || refIndex >= refs.size()) {
          throw new IOException("INODE_DIR refers to missing reference " + refIndex);
        }
        Reference r = refs.get(refIndex);
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

  private static char typeOf(FsImageProto.INodeSection.INode.Type t) throws IOException {
    switch (t) {
      case FILE:
        return 'F';
      case DIRECTORY:
        return 'D';
      case SYMLINK:
        return 'S';
      default:
        throw new IOException("unsupported inode type " + t);
    }
  }
}
