package org.hadooptraffic.sink;

import static org.junit.Assert.assertEquals;

import org.hadooptraffic.model.Locality;
import org.hadooptraffic.model.Op;
import org.hadooptraffic.model.OpIds;
import org.hadooptraffic.model.Role;
import org.hadooptraffic.model.TrafficEvent;
import org.junit.Test;

public class EventJsonTest {
  @Test
  public void formatsAllFieldsInSpecOrder() {
    TrafficEvent e = new TrafficEvent("alice", Op.READ, Role.CLIENT, Locality.REMOTE, "BP-1",
        1073741901L, 64L, 0L, 10123L, "/10.0.0.5:51234", 7L, 1762429990000L,
        1762430000123L, false, null);
    assertEquals("{\"ts\":1762430000123,\"start\":1762429990000,\"dn\":\"dn1\","
        + "\"op_id\":\"" + OpIds.format(7L) + "\",\"user\":\"alice\",\"op\":\"read\",\"role\":\"client\","
        + "\"locality\":\"remote\",\"bpid\":\"BP-1\",\"block_id\":1073741901,"
        + "\"file_block_id\":1073741901,\"bytes\":64,\"est_bytes\":0,\"dur_ms\":10123,"
        + "\"last\":false,\"peer\":\"/10.0.0.5:51234\",\"path\":null}",
        EventJson.format(e, "dn1"));
  }

  @Test
  public void escapesStrings() {
    TrafficEvent e = new TrafficEvent("we\"ird\\u\tser\n", Op.WEBHDFS_CREATE, Role.CLIENT,
        Locality.LOCAL, null, -1L, 1L, 0L, 0L, null, 1L, 0L, 0L, true,
        "/a b/\u0001ü");
    String json = EventJson.format(e, "dn1");
    assertEquals(true, json.contains("\"user\":\"we\\\"ird\\\\u\\tser\\n\""));
    assertEquals(true, json.contains("\"path\":\"/a b/\\u0001ü\""));
    assertEquals(true, json.contains("\"bpid\":null"));
    assertEquals(true, json.contains("\"block_id\":-1,\"file_block_id\":-1"));
  }
}
