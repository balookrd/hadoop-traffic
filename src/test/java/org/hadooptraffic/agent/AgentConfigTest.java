package org.hadooptraffic.agent;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AgentConfigTest {
  @Test
  public void defaultsWhenNoArgs() {
    assertEquals(AgentConfig.DEFAULT_MAX_USERS, AgentConfig.parse(null).maxUsers());
    assertEquals(AgentConfig.DEFAULT_MAX_USERS, AgentConfig.parse("  ").maxUsers());
  }

  @Test
  public void parsesMaxUsers() {
    assertEquals(5, AgentConfig.parse("maxUsers=5").maxUsers());
    assertEquals(7, AgentConfig.parse(" maxUsers = 7 ,").maxUsers());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsUnknownKey() {
    AgentConfig.parse("foo=1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonPositiveMaxUsers() {
    AgentConfig.parse("maxUsers=0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsMissingValue() {
    AgentConfig.parse("maxUsers");
  }
}
