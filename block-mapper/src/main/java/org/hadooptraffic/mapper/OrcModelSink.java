package org.hadooptraffic.mapper;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
 * Spark 3.5).
 *
 * <p>Compression dominates the run time (the reader alone is about a fifth of it), so each of
 * inode, edge and block_inode is written by {@code writers} threads, each with its own file
 * {@code part-0000k.orc}. The caller fills a {@link VectorizedRowBatch}; a full batch is handed
 * round-robin to the next writer thread and a free batch is taken from a small pool. Row order
 * across files is not kept; the consumers (Spark, Hive) read the table directory. A writer
 * thread creates its file on its first batch, so small tables do not leave empty files; file 0
 * is always created, so that every table has a schema even with no rows.
 *
 * <p>The sink methods must be called from one thread. ORC 1.5's shared memory manager only
 * calls back into the writer that checks it, so one writer per thread is safe.
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

  private static final VectorizedRowBatch END = new VectorizedRowBatch(0);

  /** One writer thread and its file; owns the ORC writer. */
  private static final class Lane extends Thread {
    final BlockingQueue<VectorizedRowBatch> queue = new ArrayBlockingQueue<>(2);
    private final Table table;
    private final Path file;
    private Writer writer;
    volatile Throwable failure;

    Lane(Table table, Path file, boolean eager) throws IOException {
      super("orc-" + file.getParent().getName() + "-" + file.getName());
      setDaemon(true);
      this.table = table;
      this.file = file;
      if (eager) {
        writer = table.createWriter(file);
      }
    }

    @Override
    public void run() {
      try {
        for (VectorizedRowBatch b = queue.take(); b != END; b = queue.take()) {
          try {
            if (failure == null) {
              if (writer == null) {
                writer = table.createWriter(file);
              }
              writer.addRowBatch(b);
            }
          } catch (Throwable t) {
            // Keep draining: the producer must never block on a dead lane.
            failure = t;
          } finally {
            b.reset();
            table.free.add(b);
          }
        }
      } catch (InterruptedException e) {
        failure = e;
      }
    }

    /** Closes the writer, if any; called after the thread has ended. */
    void closeWriter() throws IOException {
      if (writer != null) {
        writer.close();
      }
    }
  }

  /** One table: the batch being filled and the writer threads it is spread over. */
  private static final class Table {
    private final Configuration conf;
    private final TypeDescription schema;
    private final List<Lane> lanes = new ArrayList<>();
    final BlockingQueue<VectorizedRowBatch> free;
    private VectorizedRowBatch batch;
    private int next;
    private boolean closed;

    Table(Configuration conf, Path dir, String name, TypeDescription schema, int writers)
        throws IOException {
      this.conf = conf;
      this.schema = schema;
      // Two queued per lane, one being written per lane, one being filled.
      free = new ArrayBlockingQueue<>(3 * writers + 1);
      for (int i = 0; i < 3 * writers; i++) {
        free.add(schema.createRowBatch());
      }
      batch = schema.createRowBatch();
      try {
        for (int i = 0; i < writers; i++) {
          lanes.add(new Lane(this, Tables.file(dir, name, i, "orc"), i == 0));
        }
      } catch (IOException | RuntimeException e) {
        for (Lane l : lanes) {
          try {
            l.closeWriter();
          } catch (IOException | RuntimeException suppressed) {
            e.addSuppressed(suppressed);
          }
        }
        throw e;
      }
      for (Lane l : lanes) {
        l.start();
      }
    }

    Writer createWriter(Path p) throws IOException {
      return OrcFile.createWriter(p, OrcFile.writerOptions(conf)
          .setSchema(schema)
          .compress(CompressionKind.ZLIB)
          .version(OrcFile.Version.V_0_12));
    }

    /** Index of the next free row; hands a full batch to a writer thread first. */
    int nextRow() throws IOException {
      if (batch.size == batch.getMaxSize()) {
        hand(batch);
        batch = take(free);
      }
      return batch.size++;
    }

    private void hand(VectorizedRowBatch b) throws IOException {
      for (Lane l : lanes) {
        if (l.failure != null) {
          throw new IOException("ORC writer " + l.getName() + " failed", l.failure);
        }
      }
      Lane l = lanes.get(next);
      next = (next + 1) % lanes.size();
      put(l.queue, b);
    }

    /** Flushes the last batch, stops the threads and closes every file. */
    void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      IOException failure = null;
      try {
        if (batch.size > 0) {
          hand(batch);
        }
      } catch (IOException e) {
        failure = e;
      }
      for (Lane l : lanes) {
        try {
          put(l.queue, END);
          l.join();
        } catch (IOException e) {
          failure = add(failure, e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          failure = add(failure, new InterruptedIOException("interrupted closing " + l.getName()));
        }
      }
      for (Lane l : lanes) {
        if (l.failure != null) {
          failure = add(failure, new IOException("ORC writer " + l.getName() + " failed",
              l.failure));
        }
        try {
          l.closeWriter();
        } catch (IOException | RuntimeException e) {
          failure = add(failure, e instanceof IOException ? (IOException) e : new IOException(e));
        }
      }
      if (failure != null) {
        throw failure;
      }
    }

    private static IOException add(IOException first, IOException next) {
      if (first == null) {
        return next;
      }
      // The same lane failure can surface twice: from hand() and from the final check.
      boolean sameCause = first.getCause() != null && first.getCause() == next.getCause();
      if (first != next && !sameCause) {
        first.addSuppressed(next);
      }
      return first;
    }

    private static <T> void put(BlockingQueue<T> q, T v) throws IOException {
      try {
        q.put(v);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("interrupted writing ORC");
      }
    }

    private static <T> T take(BlockingQueue<T> q) throws IOException {
      try {
        return q.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("interrupted writing ORC");
      }
    }
  }

  private final Table inode;
  private final Table edge;
  private final Table blockInode;
  private final Table meta;

  public OrcModelSink(Configuration conf, Path dir) throws IOException {
    this(conf, dir, 1);
  }

  /** @param writers writer threads (and files) per large table, at least 1 */
  public OrcModelSink(Configuration conf, Path dir, int writers) throws IOException {
    if (writers < 1) {
      throw new IllegalArgumentException("writers must be at least 1: " + writers);
    }
    List<Table> opened = new ArrayList<>(4);
    try {
      opened.add(new Table(conf, dir, Tables.INODE, INODE, writers));
      opened.add(new Table(conf, dir, Tables.EDGE, EDGE, writers));
      opened.add(new Table(conf, dir, Tables.BLOCK_INODE, BLOCK_INODE, writers));
      opened.add(new Table(conf, dir, Tables.META, META, 1));
    } catch (IOException | RuntimeException e) {
      for (Table t : opened) {
        try {
          t.close();
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
