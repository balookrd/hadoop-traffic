package org.hadooptraffic.mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/**
 * ORC output: ZLIB, file version 0.12, bigint/string columns only (readable by Hive 3.1 and
 * Spark 3.5). Rows go into one VectorizedRowBatch per table; join keys carry bloom filters.
 */
public final class OrcModelSink implements InodeModelSink {
  static final TypeDescription INODE =
      TypeDescription.fromString("struct<inode_id:bigint,type:string,name:string>");
  static final TypeDescription EDGE =
      TypeDescription.fromString("struct<child_id:bigint,parent_id:bigint>");
  static final TypeDescription BLOCK_INODE =
      TypeDescription.fromString("struct<block_id:bigint,inode_id:bigint>");
  static final TypeDescription META =
      TypeDescription.fromString("struct<txid:bigint,image_file:string,parsed_at:bigint>");

  /** One table: its writer and the batch being filled. */
  private static final class Table {
    final Writer writer;
    final VectorizedRowBatch batch;

    Table(Writer writer, TypeDescription schema) {
      this.writer = writer;
      this.batch = schema.createRowBatch();
    }

    /** Index of the next free row; writes out a full batch first. */
    int nextRow() throws IOException {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      return batch.size++;
    }

    /** Flushes the last batch, then closes the writer even if the flush failed. */
    void close() throws IOException {
      try {
        if (batch.size > 0) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      } finally {
        writer.close();
      }
    }
  }

  private final Table inode;
  private final Table edge;
  private final Table blockInode;
  private final Table meta;

  public OrcModelSink(Configuration conf, Path dir) throws IOException {
    List<Table> opened = new ArrayList<>(4);
    try {
      opened.add(open(conf, Tables.file(dir, Tables.INODE, "orc"), INODE, "inode_id"));
      opened.add(open(conf, Tables.file(dir, Tables.EDGE, "orc"), EDGE, "child_id"));
      opened.add(open(conf, Tables.file(dir, Tables.BLOCK_INODE, "orc"), BLOCK_INODE,
          "block_id"));
      opened.add(open(conf, Tables.file(dir, Tables.META, "orc"), META, null));
    } catch (IOException | RuntimeException e) {
      for (Table t : opened) {
        try {
          t.writer.close();
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

  private static Table open(Configuration conf, Path p, TypeDescription schema,
      String bloomColumn) throws IOException {
    OrcFile.WriterOptions o = OrcFile.writerOptions(conf)
        .setSchema(schema)
        .compress(CompressionKind.ZLIB)
        .version(OrcFile.Version.V_0_12);
    if (bloomColumn != null) {
      o.bloomFilterColumns(bloomColumn);
    }
    return new Table(OrcFile.createWriter(p, o), schema);
  }

  private static void setLong(VectorizedRowBatch b, int col, int row, long v) {
    ((LongColumnVector) b.cols[col]).vector[row] = v;
  }

  private static void setString(VectorizedRowBatch b, int col, int row, String v) {
    ((BytesColumnVector) b.cols[col]).setVal(row, v.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void inode(long id, char type, String name) throws IOException {
    int r = inode.nextRow();
    setLong(inode.batch, 0, r, id);
    setString(inode.batch, 1, r, String.valueOf(type));
    setString(inode.batch, 2, r, name);
  }

  @Override
  public void block(long blockId, long inodeId) throws IOException {
    int r = blockInode.nextRow();
    setLong(blockInode.batch, 0, r, blockId);
    setLong(blockInode.batch, 1, r, inodeId);
  }

  @Override
  public void edge(long childId, long parentId) throws IOException {
    int r = edge.nextRow();
    setLong(edge.batch, 0, r, childId);
    setLong(edge.batch, 1, r, parentId);
  }

  @Override
  public void meta(long txid, String imageFile, long parsedAtMillis) throws IOException {
    int r = meta.nextRow();
    setLong(meta.batch, 0, r, txid);
    setString(meta.batch, 1, r, imageFile);
    setLong(meta.batch, 2, r, parsedAtMillis);
  }

  /** Closes every table; the first failure is rethrown, later ones are attached as suppressed. */
  @Override
  public void close() throws IOException {
    IOException first = null;
    RuntimeException firstRuntime = null;
    for (Table t : Arrays.asList(inode, edge, blockInode, meta)) {
      try {
        t.close();
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
}
