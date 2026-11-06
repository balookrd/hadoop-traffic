package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.hadooptraffic.it.TestTokens;
import org.junit.Test;

public class UserResolverTest {
  @Test
  public void readsUserFromLegacyToken() {
    assertEquals("alice", UserResolver.userOf(TestTokens.blockToken("alice", false)));
  }

  @Test
  public void readsUserFromProtobufToken() {
    assertEquals("alice", UserResolver.userOf(TestTokens.blockToken("alice", true)));
  }

  @Test
  public void nullTokenIsUnknown() {
    assertEquals(UserResolver.UNKNOWN, UserResolver.userOf(null));
  }

  @Test
  public void emptyIdentifierIsUnknown() {
    Token<BlockTokenIdentifier> t = new Token<>(new byte[0], new byte[0], new Text(""),
        new Text(""));
    assertEquals(UserResolver.UNKNOWN, UserResolver.userOf(t));
  }

  @Test
  public void garbageIdentifierDoesNotThrow() {
    Token<BlockTokenIdentifier> t = new Token<>(new byte[] {(byte) 0xFF, 1, 2}, new byte[0],
        new Text(""), new Text(""));
    assertNotNull(UserResolver.userOf(t));
  }
}
