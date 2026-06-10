# replayd Architecture

```mermaid
flowchart TB
    subgraph prod ["Production Traffic"]
        C[Client] <-.->|TLS| ST["stunnel (MITM)"]
        ST <-.->|TLS| DB[(RedisDB)(:6381)]
        C -.-x|"original TLS broken,<br>redirected through stunnel"| DB
    end

    ST -->|"tcpdump · plain-text PCAP"| RP

    subgraph rp ["replayd"]
        RP[":8888 PCAP listener"]
        BE[":8001 backend (fake RedisDB)"]
    end

    subgraph eden ["Eden Server (proxy under test)"]
        EF[":6365 client-facing"]
        EB["backend connection"]
    end

    RP -->|"1 · replay client requests"| EF
    EB -->|"2 · Eden forwards to backend"| BE
    BE -->|"3 · respond with recorded data"| EB
    EF -->|"4 · Eden returns response ✓"| RP
```

## Flow

1. **Capture**: `stunnel` sits between Client and RedisDB, terminating TLS on both
   sides. `tcpdump` captures the decrypted plain-text traffic as a PCAP stream.

2. **Stream**: The PCAP is sent to replayd (`cat capture.pcap | nc localhost 8888`).
   replayd parses it into request/response exchanges.

3. **Replay**: replayd connects to Eden's client-facing port (`:6365`) and sends
   the original client requests. Eden treats this as a normal client session.

4. **Intercept**: Eden forwards requests to its "backend", which is actually
   replayd's backend port (`:8001`). replayd recognizes the forwarded data and
   responds with the original RedisDB responses from the PCAP.

5. **Verify**: Eden returns the response to replayd's client connection. replayd
   compares it byte-for-byte against the original PCAP response to verify Eden's
   proxy transparency.
