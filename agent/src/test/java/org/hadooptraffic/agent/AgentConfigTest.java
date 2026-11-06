package org.hadooptraffic.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AgentConfigTest {
  @Test
  public void defaultsWhenNoArgs() {
    assertEquals(AgentConfig.DEFAULT_MAX_USERS, AgentConfig.parse(null).maxUsers());
    assertEquals(AgentConfig.DEFAULT_MAX_USERS, AgentConfig.parse("  ").maxUsers());
  }

  @Test
  public void userMetricsOffByDefault() {
    assertFalse(AgentConfig.parse(null).userMetrics());
    assertFalse(AgentConfig.parse("userMetrics=false").userMetrics());
    assertTrue(AgentConfig.parse("userMetrics=true,maxUsers=5").userMetrics());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonBooleanUserMetrics() {
    AgentConfig.parse("userMetrics=yes");
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

  @Test
  public void eventDefaults() {
    AgentConfig c = AgentConfig.parse(null);
    assertNull(c.eventsPath());
    assertEquals(65536, c.eventsQueue());
    assertEquals(256, c.eventsMaxFileMb());
    assertEquals(300, c.eventsRotateSec());
    assertEquals(100, c.eventsMaxFiles());
    assertEquals(10, c.flushIntervalSec());
  }

  @Test
  public void parsesEventOptions() {
    AgentConfig c = AgentConfig.parse("maxUsers=5,events=/var/log/t/events.jsonl,eventsQueue=10,"
        + "eventsMaxFileMb=1,eventsRotateSec=30,eventsMaxFiles=3,flushIntervalSec=0");
    assertEquals(5, c.maxUsers());
    assertEquals("/var/log/t/events.jsonl", c.eventsPath());
    assertEquals(10, c.eventsQueue());
    assertEquals(1, c.eventsMaxFileMb());
    assertEquals(30, c.eventsRotateSec());
    assertEquals(3, c.eventsMaxFiles());
    assertEquals(0, c.flushIntervalSec());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsEmptyEventsPath() {
    AgentConfig.parse("events=");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonPositiveQueue() {
    AgentConfig.parse("eventsQueue=0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNegativeFlushInterval() {
    AgentConfig.parse("flushIntervalSec=-1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonNumericValue() {
    AgentConfig.parse("eventsMaxFiles=many");
  }
}
