package org.hadooptraffic.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

/** Reads the four tables back (either format) and resolves paths. Tests only. */
final class ModelFiles {
  static final long ROOT = 16385L;

  final Map<Long, String> names = new HashMap<>();
  final Map<Long, Character> types = new HashMap<>();
  final Map<Long, Long> parents = new HashMap<>();
  final Map<Long, Long> blockInodes = new HashMap<>();
  long txid;
  String imageFile;

  static ModelFiles read(Configuration conf, Path dir, MapperOptions.Format f) throws IOException {
    ModelFiles m = new ModelFiles();
    if (f == MapperOptions.Format.ORC) {
      for (Object[] r : orcTable(conf, dir, Tables.INODE)) {
        long id = (Long) r[0];
        m.types.put(id, ((String) r[1]).charAt(0));
        m.names.put(id, (String) r[2]);
      }
      for (Object[] r : orcTable(conf, dir, Tables.EDGE)) {
        m.parents.put((Long) r[0], (Long) r[1]);
      }
      for (Object[] r : orcTable(conf, dir, Tables.BLOCK_INODE)) {
        m.blockInodes.put((Long) r[0], (Long) r[1]);
      }
      Object[] meta = orcTable(conf, dir, Tables.META).get(0);
      m.txid = (Long) meta[0];
      m.imageFile = (String) meta[1];
    } else {
      for (String[] r : tsv(conf, Tables.file(dir, Tables.INODE, "tsv"))) {
        long id = Long.parseLong(r[0]);
        m.types.put(id, r[1].charAt(0));
        m.names.put(id, r[2]);
      }
      for (String[] r : tsv(conf, Tables.file(dir, Tables.EDGE, "tsv"))) {
        m.parents.put(Long.parseLong(r[0]), Long.parseLong(r[1]));
      }
      for (String[] r : tsv(conf, Tables.file(dir, Tables.BLOCK_INODE, "tsv"))) {
        m.blockInodes.put(Long.parseLong(r[0]), Long.parseLong(r[1]));
      }
      String[] meta = tsv(conf, Tables.file(dir, Tables.META, "tsv")).get(0);
      m.txid = Long.parseLong(meta[0]);
      m.imageFile = meta[1];
    }
    return m;
  }

  /** Absolute path of an inode, or null when its chain does not reach the root. */
  String path(long inodeId) {
    if (inodeId == ROOT) {
      return "/";
    }
    StringBuilder sb = new StringBuilder();
    long cur = inodeId;
    for (int depth = 0; depth < 10_000; depth++) {
      String name = names.get(cur);
      if (name == null) {
        return null;
      }
      sb.insert(0, "/" + name);
      Long parent = parents.get(cur);
      if (parent == null) {
        return null;
      }
      if (parent == ROOT) {
        return sb.toString();
      }
      cur = parent;
    }
    return null;
  }

  /** All rows of every part file of a table. */
  static List<Object[]> orcTable(Configuration conf, Path dir, String table) throws IOException {
    List<Object[]> out = new ArrayList<>();
    FileSystem fs = dir.getFileSystem(conf);
    for (org.apache.hadoop.fs.FileStatus st : fs.listStatus(new Path(dir, table))) {
      if (st.getPath().getName().matches("part-\\d{5}\\.orc")) {
        out.addAll(orc(conf, st.getPath()));
      }
    }
    return out;
  }

  /** All rows of an ORC file; bigint columns as Long, string columns as String. */
  private static List<Object[]> orc(Configuration conf, Path p) throws IOException {
    List<Object[]> out = new ArrayList<>();
    Reader reader = OrcFile.createReader(p, OrcFile.readerOptions(conf));
    VectorizedRowBatch b = reader.getSchema().createRowBatch();
    try (RecordReader rows = reader.rows()) {
      while (rows.nextBatch(b)) {
        for (int r = 0; r < b.size; r++) {
          Object[] row = new Object[b.numCols];
          for (int c = 0; c < b.numCols; c++) {
            ColumnVector v = b.cols[c];
            int i = v.isRepeating ? 0 : r;
            if (v instanceof LongColumnVector) {
              row[c] = ((LongColumnVector) v).vector[i];
            } else {
              BytesColumnVector s = (BytesColumnVector) v;
              row[c] = new String(s.vector[i], s.start[i], s.length[i], StandardCharsets.UTF_8);
            }
          }
          out.add(row);
        }
      }
    }
    return out;
  }

  private static List<String[]> tsv(Configuration conf, Path p) throws IOException {
    List<String[]> out = new ArrayList<>();
    FileSystem fs = p.getFileSystem(conf);
    try (BufferedReader r = new BufferedReader(
        new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
      for (String line = r.readLine(); line != null; line = r.readLine()) {
        String[] cols = line.split("\t", -1);
        for (int i = 0; i < cols.length; i++) {
          cols[i] = TsvModelSink.unescape(cols[i]);
        }
        out.add(cols);
      }
    }
    return out;
  }
}
