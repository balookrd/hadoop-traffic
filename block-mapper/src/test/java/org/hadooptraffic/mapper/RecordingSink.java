package org.hadooptraffic.mapper;

import java.util.HashMap;
import java.util.Map;

/** Keeps everything in memory. Tests only. */
final class RecordingSink implements InodeModelSink {
  final Map<Long, String> names = new HashMap<>();
  final Map<Long, Character> types = new HashMap<>();
  final Map<Long, Long> parents = new HashMap<>();
  final Map<Long, Long> blockInodes = new HashMap<>();
  long txid = -1;
  String imageFile;
  int metaCalls;

  @Override
  public void inode(long id, char type, String name) {
    names.put(id, name);
    types.put(id, type);
  }

  @Override
  public void block(long blockId, long inodeId) {
    blockInodes.put(blockId, inodeId);
  }

  @Override
  public void edge(long childId, long parentId) {
    parents.put(childId, parentId);
  }

  @Override
  public void meta(long t, String file, long parsedAtMillis) {
    txid = t;
    imageFile = file;
    metaCalls++;
  }

  @Override
  public void close() {
  }

  String path(long inodeId) {
    if (inodeId == ModelFiles.ROOT) {
      return "/";
    }
    StringBuilder sb = new StringBuilder();
    long cur = inodeId;
    for (int depth = 0; depth < 10_000; depth++) {
      String name = names.get(cur);
      Long parent = parents.get(cur);
      if (name == null || parent == null) {
        return null;
      }
      sb.insert(0, "/" + name);
      if (parent == ModelFiles.ROOT) {
        return sb.toString();
      }
      cur = parent;
    }
    return null;
  }
}
