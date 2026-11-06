package org.hadooptraffic.it;

import java.util.EnumSet;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;

/** Builds real serialized block tokens for unit tests. */
public final class TestTokens {
  private TestTokens() {
  }

  public static Token<BlockTokenIdentifier> blockToken(String user, boolean proto) {
    BlockTokenIdentifier id = new BlockTokenIdentifier(user, "bp-1", 42L,
        EnumSet.of(BlockTokenIdentifier.AccessMode.READ), new StorageType[0], new String[0],
        proto);
    return new Token<>(id.getBytes(), new byte[0], id.getKind(), new Text(""));
  }
}
