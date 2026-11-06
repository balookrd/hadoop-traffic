package org.hadooptraffic.mapper;

import java.io.Closeable;
import java.io.IOException;

/** Receives the inode model while an fsimage is read. Not thread-safe. */
public interface InodeModelSink extends Closeable {
  /** One inode; type is 'F' (file), 'D' (directory) or 'S' (symlink); root's name is "". */
  void inode(long id, char type, String name) throws IOException;

  /** One block of a file (striped files store the block group id). */
  void block(long blockId, long inodeId) throws IOException;

  /** Current parent of an inode. */
  void edge(long childId, long parentId) throws IOException;

  /** Image metadata; written once, last. */
  void meta(long txid, String imageFile, long parsedAtMillis) throws IOException;
}
