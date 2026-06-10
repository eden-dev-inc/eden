# replayd

**replayd** is a PCAP-driven proxy verification tool. It replays captured network traffic to test whether a proxy server passes requests and responses through **unchanged**.

---

## Overview

Given a PCAP capture of traffic between a client and a backend service (e.g. RedisDB), replayd:

1. **Reads the PCAP stream** and splits traffic into *incoming* (client→server) and *outgoing* (server→client) directions based on port.
2. **Connects to the system under test** (the proxy, e.g. Eden) and sends the captured incoming packets as real requests.
3. **Listens for a callback connection** from the proxy. The proxy, believing it is talking to the real backend, connects back to replayd.
4. **Responds to the proxy's backend requests** with the outgoing PCAP data that followed each corresponding request in the original capture.
5. **Verifies correctness** at both ends:
   - The request the proxy forwarded to the backend must exactly match what replayd originally sent to the proxy.
   - The response the proxy returned to the client must exactly match what replayd sent back as the backend.

If all requests and responses pass through the proxy byte-for-byte, the proxy is verified as transparent.

---

## How It Works

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                          replayd                                    │
  │                                                                     │
  │  PCAP stream                                                        │
  │      │                                                              │
  │      ▼                                                              │
  │  ┌────────────┐   "incoming" packets    ┌──────────────────────┐   │
  │  │  PCAP      │ ──────────────────────► │  Proxy (Eden)        │   │
  │  │  parser    │                         │                      │   │
  │  └────────────┘ ◄────────────────────── │  (forwards to        │   │
  │      │           proxy's client resp    │   replayd backend    │   │
  │      │                                  │   listener)          │   │
  │      │                                  └──────────┬───────────┘   │
  │      │                                             │               │
  │      │          backend request (forwarded)        │               │
  │      │ ◄───────────────────────────────────────────┘               │
  │      │                                                              │
  │      ▼                                                              │
  │  ┌────────────┐                                                     │
  │  │  backend   │  "outgoing" PCAP data ──────────────────────────►  │
  │  │  listener  │  (response to proxy's backend request)             │
  │  └────────────┘                                                     │
  │                                                                     │
  │  ┌────────────────────────────────┐                                 │
  │  │  verifier                      │                                 │
  │  │  · request sent == request     │                                 │
  │  │    received by backend         │                                 │
  │  │  · response sent == response   │                                 │
  │  │    received by client          │                                 │
  │  └────────────────────────────────┘                                 │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## Example: Verifying a Redis Proxy

Suppose you have a proxy (Eden) that sits between application clients and a RedisDB instance. You want to verify that Eden forwards all traffic transparently, with no modification and no dropped bytes.

### Step 1: Capture traffic on the Redis machine

```bash
tcpdump -i eth0 -w redis-capture.pcap port 6379
```

Run your workload, then stop the capture.

### Step 2: Stream the PCAP to replayd

```bash
cat redis-capture.pcap | replayd \
  --backend-port 6379 \
  --proxy-addr eden.internal:6379 \
  --listen-addr 0.0.0.0:16379
```

| Flag | Description |
|------|-------------|
| `--backend-port` | Port used to identify traffic direction in the PCAP (e.g. `6379` for Redis) |
| `--proxy-addr` | Address of the proxy under test (Eden) |
| `--listen-addr` | Address replayd listens on, acting as the backend for the proxy |

### Step 3: Configure Eden

Point Eden's backend connection at replayd's listener address (`--listen-addr` above) instead of the real Redis instance. Eden should require no other changes; it will connect to replayd as if it were Redis.

### Step 4: Read the results

replayd prints a verification report when the PCAP stream ends:

```
replayd verification report
────────────────────────────────────────
  Total exchanges:       142
  Passed (exact match):  142
  Failed:                  0

✓ All requests and responses passed through unchanged.
```

If any exchange fails, replayd reports the index, the expected bytes, and the actual bytes received, so mismatches are easy to diagnose.

---

## Verification Logic

For each request/response exchange in the PCAP:

```
1.  replayd sends REQUEST  ──────────────────────► Eden (proxy)
                                                      │
                                                      │ (forwarded)
                                                      ▼
2.  replayd receives REQUEST ◄─────────────────── Eden (acting as client to backend)

    ASSERT: received request == sent request

3.  replayd sends RESPONSE  ─────────────────────► Eden (backend reply)
                                                      │
                                                      │ (forwarded)
                                                      ▼
4.  replayd receives RESPONSE ◄──────────────────── Eden (proxy response to client)

    ASSERT: received response == sent response
```

A proxy passes verification only if **both** assertions hold for every exchange in the capture.

---

## PCAP Direction Detection

replayd determines packet direction from the PCAP by examining the destination port:

- Packets **to** `--backend-port` → **incoming** (client requests)
- Packets **from** `--backend-port` → **outgoing** (server responses)

The IP addresses in the capture are ignored; only port and direction are used. This means the same PCAP can be replayed against a proxy on any host.

---

## PCAP Input

replayd reads from **stdin**, so any source that produces a valid PCAP byte stream works:

```bash
# From a file
cat capture.pcap | replayd ...

# Live capture piped directly
tcpdump -i eth0 -w - port 6379 | replayd ...

# Over SSH from a remote host
ssh user@redis-host "tcpdump -i eth0 -w - port 6379" | replayd ...
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All exchanges verified successfully |
| `1` | One or more verification failures |
| `2` | PCAP parse error or connection failure |

---

## Roadmap

- [ ] TLS support (pass-through and termination)
- [ ] Configurable matching tolerance (e.g. ignore specific header fields)
- [ ] Multiple concurrent PCAP sessions
- [ ] HTML / JSON verification reports
- [ ] Named protocol support (Redis, PostgreSQL wire protocol, MySQL, etc.) for human-readable diffs on failure

---

## License

MIT
