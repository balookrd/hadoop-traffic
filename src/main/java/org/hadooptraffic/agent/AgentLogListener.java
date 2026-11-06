package org.hadooptraffic.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.utility.JavaModule;

/** Reports instrumented classes and instrumentation failures to the DataNode log. */
final class AgentLogListener extends AgentBuilder.Listener.Adapter {
  @Override
  public void onTransformation(TypeDescription type, ClassLoader loader, JavaModule module,
      boolean loaded, DynamicType dynamicType) {
    AgentLog.info("hdfs-traffic-agent: instrumented " + type.getName());
  }

  @Override
  public void onError(String typeName, ClassLoader loader, JavaModule module, boolean loaded,
      Throwable error) {
    AgentLog.warn("hdfs-traffic-agent: failed to instrument " + typeName
        + "; traffic of this class is not accounted", error);
  }
}
