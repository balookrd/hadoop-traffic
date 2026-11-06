package org.hadooptraffic.context;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/** Extracts the effective user from a block access token. */
public final class UserResolver {
  public static final String UNKNOWN = "__unknown__";

  private UserResolver() {
  }

  public static String userOf(Token<?> token) {
    if (token == null) {
      return UNKNOWN;
    }
    byte[] ident = token.getIdentifier();
    if (ident == null || ident.length == 0) {
      return UNKNOWN;
    }
    try {
      BlockTokenIdentifier id = new BlockTokenIdentifier();
      // readFields handles both legacy and protobuf encodings; it needs a mark-capable stream.
      id.readFields(new DataInputStream(new ByteArrayInputStream(ident)));
      String user = id.getUserId();
      return user == null || user.isEmpty() ? UNKNOWN : user;
    } catch (IOException | RuntimeException e) {
      return UNKNOWN;
    }
  }
}
