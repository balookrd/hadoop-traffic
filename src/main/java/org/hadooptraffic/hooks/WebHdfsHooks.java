package org.hadooptraffic.hooks;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedStream;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.hadooptraffic.context.LocalityResolver;
import org.hadooptraffic.context.OpContext;
import org.hadooptraffic.context.UserResolver;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.Role;

/**
 * WebHDFS HTTP-leg accounting. A request handler method (onOpen/onCreate/onAppend) opens a
 * pending context on its thread; the read stream / HdfsWriter created inside it is bound to
 * that context and carries it until the transfer ends.
 */
public final class WebHdfsHooks {
  /** Task id injected into the DN-embedded DFSClient: client name becomes DFSClient_WEBHDFS_*. */
  public static final String TASK_ID = "WEBHDFS";
  private static final String TASK_ID_KEY = "mapreduce.task.attempt.id";

  private static final ThreadLocal<OpContext> PENDING = new ThreadLocal<>();
  // Weak keys: a stream that is never closed must not leak its context.
  private static final Map<Object, OpContext> BOUND =
      Collections.synchronizedMap(new WeakHashMap<>());

  private WebHdfsHooks() {
  }

  public static boolean onRequestEnter(String method, UserGroupInformation ugi,
      ChannelHandlerContext ctx) {
    Op op = Op.fromWebHdfsMethod(method);
    if (op == null) {
      return false;
    }
    String user = ugi == null ? UserResolver.UNKNOWN : ugi.getShortUserName();
    SocketAddress remote = ctx == null ? null : ctx.channel().remoteAddress();
    PENDING.set(new OpContext(user, op, Role.CLIENT, LocalityResolver.of(remote), null, -1L,
        remote == null ? null : remote.toString()));
    return true;
  }

  /** Drops the pending context; if nothing was bound, no data was transferred. */
  public static void onRequestExit(boolean started) {
    if (started) {
      PENDING.remove();
    }
  }

  /** Copies conf with the WebHDFS task id when called inside a request; otherwise returns it. */
  public static Configuration tagClientConf(Configuration conf) {
    if (conf == null || PENDING.get() == null) {
      return conf;
    }
    Configuration copy = new Configuration(conf);
    copy.set(TASK_ID_KEY, TASK_ID);
    return copy;
  }

  public static void bindPending(Object owner) {
    OpContext ctx = PENDING.get();
    if (ctx != null && owner != null) {
      BOUND.put(owner, ctx);
    }
  }

  public static void addBytes(Object owner, long n) {
    if (n <= 0) {
      return;
    }
    OpContext ctx = BOUND.get(owner);
    if (ctx != null) {
      ctx.addBytes(n);
    }
  }

  public static void finish(Object owner) {
    OpContext ctx = BOUND.remove(owner);
    if (ctx != null) {
      ctx.finish();
    }
  }

  /** Read path: the byte count comes from the Netty stream itself. */
  public static void finishRead(ChunkedStream stream) {
    OpContext ctx = BOUND.remove(stream);
    if (ctx != null) {
      ctx.addBytes(stream.transferredBytes());
      ctx.finish();
    }
  }
}
