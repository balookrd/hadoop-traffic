package org.hadooptraffic.mapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Tab-separated text, for tests and manual inspection. In names, backslash, tab, newline and
 * carriage return are escaped as {@code \\}, {@code \t}, {@code \n}, {@code \r}.
 */
public final class TsvModelSink implements InodeModelSink {
  private final Writer inode;
  private final Writer edge;
  private final Writer blockInode;
  private final Writer meta;

  public TsvModelSink(FileSystem fs, Path dir) throws IOException {
    List<Writer> opened = new ArrayList<>(4);
    try {
      opened.add(open(fs, Tables.file(dir, Tables.INODE, "tsv")));
      opened.add(open(fs, Tables.file(dir, Tables.EDGE, "tsv")));
      opened.add(open(fs, Tables.file(dir, Tables.BLOCK_INODE, "tsv")));
      opened.add(open(fs, Tables.file(dir, Tables.META, "tsv")));
    } catch (IOException | RuntimeException e) {
      for (Writer w : opened) {
        try {
          w.close();
        } catch (IOException | RuntimeException suppressed) {
          e.addSuppressed(suppressed);
        }
      }
      throw e;
    }
    inode = opened.get(0);
    edge = opened.get(1);
    blockInode = opened.get(2);
    meta = opened.get(3);
  }

  private static Writer open(FileSystem fs, Path p) throws IOException {
    return new BufferedWriter(new OutputStreamWriter(fs.create(p, false), StandardCharsets.UTF_8),
        1 << 16);
  }

  @Override
  public void inode(long id, char type, String name) throws IOException {
    inode.write(id + "\t" + type + "\t" + escape(name) + "\n");
  }

  @Override
  public void block(long blockId, long inodeId) throws IOException {
    blockInode.write(blockId + "\t" + inodeId + "\n");
  }

  @Override
  public void edge(long childId, long parentId) throws IOException {
    edge.write(childId + "\t" + parentId + "\n");
  }

  @Override
  public void meta(long txid, String imageFile, long parsedAtMillis) throws IOException {
    meta.write(txid + "\t" + escape(imageFile) + "\t" + parsedAtMillis + "\n");
  }

  /** Closes every writer; the first failure is rethrown, later ones are attached as suppressed. */
  @Override
  public void close() throws IOException {
    IOException first = null;
    RuntimeException firstRuntime = null;
    for (Writer w : new Writer[] {inode, edge, blockInode, meta}) {
      try {
        w.close();
      } catch (IOException | RuntimeException e) {
        if (first == null && firstRuntime == null) {
          if (e instanceof RuntimeException) {
            firstRuntime = (RuntimeException) e;
          } else {
            first = (IOException) e;
          }
        } else if (first != null) {
          first.addSuppressed(e);
        } else {
          firstRuntime.addSuppressed(e);
        }
      }
    }
    if (firstRuntime != null) {
      throw firstRuntime;
    }
    if (first != null) {
      throw first;
    }
  }

  static String escape(String s) {
    StringBuilder sb = new StringBuilder(s.length() + 8);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\':
          sb.append("\\\\");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        default:
          sb.append(c);
      }
    }
    return sb.toString();
  }

  static String unescape(String s) {
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' && i + 1 < s.length()) {
        char n = s.charAt(++i);
        sb.append(n == 't' ? '\t' : n == 'n' ? '\n' : n == 'r' ? '\r' : n);
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
