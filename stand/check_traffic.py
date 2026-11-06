#!/usr/bin/env python3
"""Read UserTraffic counters from the stand's DataNodes over /jmx.

  check_traffic.py snapshot              -> JSON of counters summed over all DataNodes
  check_traffic.py verify BASELINE.json  -> wait until the delta since BASELINE matches the
                                            scenario's expectations, print the delta table
"""
import json
import re
import sys
import time
import urllib.request

DATANODES = {"tr-dn1": 29864, "tr-dn2": 29865, "tr-dn3": 29866}
BEAN = "Hadoop:service=DataNode,name=UserTraffic"
MIB = 1024 * 1024

# (user, op, role, locality or None for any) -> exact payload bytes the scenario produces.
EXPECTED = {
    ("alice", "write", "client", "remote"): 3 * MIB,
    ("alice", "write", "pipeline", "remote"): 6 * MIB,
    ("dave", "write", "client", "remote"): 1 * MIB,
    ("hdfs", "write", "datanode", "remote"): 2 * MIB,
    ("bob", "read", "client", "remote"): 3 * MIB,
    ("carol", "read", "client", "local"): 3 * MIB,
    ("bob", "webhdfs_create", "client", "remote"): 2 * MIB,
    ("bob", "webhdfs_read", "client", "remote"): 2 * MIB,
    ("bob", "write", "webhdfs_proxy", None): 2 * MIB,
    ("bob", "read", "webhdfs_proxy", None): 2 * MIB,
    ("bob", "write", "pipeline", "remote"): 4 * MIB,
}
# Re-replication: the source DataNode sends at least one copy of dave's 1 MiB block.
AT_LEAST = {
    ("hdfs", "transfer", "datanode", None): 1 * MIB,
}


def fetch():
    """Counter name -> value, summed over DataNodes."""
    total = {}
    for host, port in DATANODES.items():
        url = "http://localhost:%d/jmx?qry=%s" % (port, BEAN)
        with urllib.request.urlopen(url, timeout=5) as resp:
            beans = json.load(resp).get("beans", [])
        if not beans:
            raise RuntimeError("%s: no %s bean (agent not attached?)" % (host, BEAN))
        for name, value in beans[0].items():
            if ".user." in name:
                total[name] = total.get(name, 0) + int(value)
    return total


NAME = re.compile(r"^(Bytes|EstimatedBytes|Ops)\.user\.(.+)\.op\.(\w+)\.role\.(\w+)"
                  r"\.locality\.(\w+)$")


def parse(name):
    """'Bytes.user.u.op.o.role.r.locality.l' -> ('Bytes', (u, o, r, l))."""
    m = NAME.match(name)
    if not m:
        raise ValueError("unexpected UserTraffic attribute: " + name)
    return m.group(1), m.groups()[1:]


def delta(baseline, current):
    out = {}
    for name, value in current.items():
        diff = value - baseline.get(name, 0)
        if diff:
            counter, key = parse(name)
            out.setdefault(key, {})[counter] = diff
    return out


def bytes_for(table, user, op, role, locality):
    return sum(v.get("Bytes", 0) for (u, o, r, l), v in table.items()
               if (u, o, r) == (user, op, role) and (locality is None or l == locality))


def failures(table):
    bad = []
    for key, want in EXPECTED.items():
        got = bytes_for(table, *key)
        if got != want:
            bad.append("%s: Bytes %d, expected %d" % (key, got, want))
    for key, want in AT_LEAST.items():
        got = bytes_for(table, *key)
        if got < want:
            bad.append("%s: Bytes %d, expected >= %d" % (key, got, want))
    return bad


def show(table):
    print("%-8s %-15s %-14s %-7s %12s %10s %5s"
          % ("user", "op", "role", "local", "Bytes", "EstBytes", "Ops"))
    for key in sorted(table):
        v = table[key]
        print("%-8s %-15s %-14s %-7s %12d %10d %5d"
              % (key + (v.get("Bytes", 0), v.get("EstimatedBytes", 0), v.get("Ops", 0))))


def main():
    if sys.argv[1:2] == ["snapshot"]:
        json.dump(fetch(), sys.stdout)
        return 0
    if len(sys.argv) == 3 and sys.argv[1] == "verify":
        with open(sys.argv[2]) as f:
            baseline = json.load(f)
        # metrics2 caches JMX values for about one metrics period (10 s), and write bytes are
        # reported when the block is closed: poll instead of reading once.
        deadline = time.time() + 90
        while True:
            table = delta(baseline, fetch())
            bad = failures(table)
            if not bad or time.time() > deadline:
                break
            time.sleep(3)
        show(table)
        if bad:
            print("\nFAIL:\n  " + "\n  ".join(bad))
            return 1
        print("\nOK: all %d expectations met" % (len(EXPECTED) + len(AT_LEAST)))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
