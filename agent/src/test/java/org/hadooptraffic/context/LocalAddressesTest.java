package org.hadooptraffic.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.hadooptraffic.testutil.FakeClock;
import org.junit.Test;

public class LocalAddressesTest {
  private static InetAddress ip(String s) throws Exception {
    return InetAddress.getByName(s);
  }

  /** Source returning a mutable set; counts reads and can be told to fail. */
  private static final class FakeSource implements LocalAddresses.Source {
    final Set<InetAddress> addresses = new HashSet<>();
    final AtomicInteger reads = new AtomicInteger();
    volatile boolean fail;

    @Override
    public Set<InetAddress> read() throws Exception {
      reads.incrementAndGet();
      if (fail) {
        throw new IOException("boom");
      }
      return new HashSet<>(addresses);
    }
  }

  @Test
  public void readsOnceWithinTheRefreshInterval() throws Exception {
    FakeSource src = new FakeSource();
    src.addresses.add(ip("10.0.0.5"));
    FakeClock clock = new FakeClock(0);
    LocalAddresses la = new LocalAddresses(src, clock, 60);

    for (int i = 0; i < 1000; i++) {
      assertTrue(la.contains(ip("10.0.0.5")));
      assertFalse(la.contains(ip("10.0.0.6")));
    }
    clock.advanceSec(59);
    la.contains(ip("10.0.0.5"));
    assertEquals(1, src.reads.get());
  }

  @Test
  public void picksUpNewAddressesAfterTheInterval() throws Exception {
    FakeSource src = new FakeSource();
    FakeClock clock = new FakeClock(0);
    LocalAddresses la = new LocalAddresses(src, clock, 60);
    assertFalse(la.contains(ip("10.0.0.7")));

    src.addresses.add(ip("10.0.0.7"));
    assertFalse(la.contains(ip("10.0.0.7")));
    clock.advanceSec(60);
    assertTrue(la.contains(ip("10.0.0.7")));
    assertEquals(2, src.reads.get());
  }

  @Test
  public void failedRefreshKeepsPreviousAddresses() throws Exception {
    FakeSource src = new FakeSource();
    src.addresses.add(ip("10.0.0.8"));
    FakeClock clock = new FakeClock(0);
    LocalAddresses la = new LocalAddresses(src, clock, 60);
    assertTrue(la.contains(ip("10.0.0.8")));

    src.fail = true;
    clock.advanceSec(60);
    assertTrue(la.contains(ip("10.0.0.8")));
    // The failed read counts as a refresh: no retry on every call.
    la.contains(ip("10.0.0.8"));
    assertEquals(2, src.reads.get());
  }

  @Test
  public void failedFirstReadMeansNothingIsLocal() throws Exception {
    FakeSource src = new FakeSource();
    src.fail = true;
    LocalAddresses la = new LocalAddresses(src, new FakeClock(0), 60);
    assertFalse(la.contains(ip("10.0.0.9")));
  }

  @Test
  public void systemSetContainsEveryInterfaceAddress() throws Exception {
    LocalAddresses la = LocalAddresses.system();
    for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
      for (InetAddress a : Collections.list(ni.getInetAddresses())) {
        assertTrue(a.toString(), la.contains(a));
      }
    }
    assertFalse(la.contains(ip("192.0.2.1")));
  }
}
