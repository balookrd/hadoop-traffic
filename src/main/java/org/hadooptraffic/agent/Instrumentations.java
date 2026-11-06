package org.hadooptraffic.agent;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.namedOneOf;
import static net.bytebuddy.matcher.ElementMatchers.returns;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.hadooptraffic.advice.DataTransferAdvice;
import org.hadooptraffic.advice.HdfsWriterContentAdvice;
import org.hadooptraffic.advice.HdfsWriterCtorAdvice;
import org.hadooptraffic.advice.HdfsWriterReleaseAdvice;
import org.hadooptraffic.advice.NewDfsClientAdvice;
import org.hadooptraffic.advice.ReceivePacketAdvice;
import org.hadooptraffic.advice.SendPacketAdvice;
import org.hadooptraffic.advice.StartDataNodeAdvice;
import org.hadooptraffic.advice.WebHdfsReadStreamCloseAdvice;
import org.hadooptraffic.advice.WebHdfsReadStreamCtorAdvice;
import org.hadooptraffic.advice.WebHdfsRequestAdvice;
import org.hadooptraffic.advice.XceiverOpAdvice;

/** Declares which Hadoop methods get which advice. */
final class Instrumentations {
  static final String DN = "org.apache.hadoop.hdfs.server.datanode.";
  static final String WEB = DN + "web.webhdfs.";

  private Instrumentations() {
  }

  static AgentBuilder builder() {
    return new AgentBuilder.Default()
        .disableClassFormatChanges()
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .with(new AgentLogListener())
        .ignore(nameStartsWith("net.bytebuddy.").or(nameStartsWith("org.hadooptraffic.")))
        .type(named(DN + "DataXceiver"))
        .transform(advice(XceiverOpAdvice.class, isPublic().and(namedOneOf("readBlock",
            "writeBlock", "transferBlock", "copyBlock", "replaceBlock",
            "requestShortCircuitFds"))))
        .type(named(DN + "BlockSender"))
        .transform(advice(SendPacketAdvice.class,
            named("sendPacket").and(returns(int.class))))
        .type(named(DN + "BlockReceiver"))
        .transform(advice(ReceivePacketAdvice.class,
            named("receivePacket").and(takesArguments(0)).and(returns(int.class))))
        .type(named(DN + "DataNode$DataTransfer"))
        .transform(advice(DataTransferAdvice.class, named("run").and(takesArguments(0))))
        .type(named(DN + "DataNode"))
        .transform(advice(StartDataNodeAdvice.class, named("startDataNode")))
        .type(named(WEB + "WebHdfsHandler"))
        .transform(advice(WebHdfsRequestAdvice.class,
            namedOneOf("onOpen", "onCreate", "onAppend").and(takesArguments(1))))
        .transform(advice(NewDfsClientAdvice.class,
            named("newDfsClient").and(takesArguments(2))))
        .type(nameStartsWith(WEB + "WebHdfsHandler$")
            .and(hasSuperType(named("io.netty.handler.stream.ChunkedStream"))))
        .transform(advice(WebHdfsReadStreamCtorAdvice.class, isConstructor()))
        .transform(advice(WebHdfsReadStreamCloseAdvice.class,
            named("close").and(takesArguments(0))))
        .type(named(WEB + "HdfsWriter"))
        .transform(advice(HdfsWriterCtorAdvice.class, isConstructor()))
        .transform(advice(HdfsWriterContentAdvice.class, named("channelRead0")
            .and(takesArgument(1, named("io.netty.handler.codec.http.HttpContent")))))
        .transform(advice(HdfsWriterReleaseAdvice.class, named("releaseDfsResources")));
  }

  /**
   * Applies advice to matching methods; if nothing matches (signature drift in a Hadoop
   * version), logs a WARN and leaves the class untouched so only this hook is disabled.
   */
  static AgentBuilder.Transformer advice(Class<?> adviceClass,
      ElementMatcher<? super MethodDescription> methods) {
    return (builder, type, classLoader, module, protectionDomain) -> {
      if (type.getDeclaredMethods().filter(methods).isEmpty()) {
        AgentLog.warn("hdfs-traffic-agent: no method matched " + adviceClass.getSimpleName()
            + " in " + type.getName() + "; this hook is disabled", null);
        return builder;
      }
      return builder.visit(Advice.to(adviceClass).on(methods));
    };
  }
}
