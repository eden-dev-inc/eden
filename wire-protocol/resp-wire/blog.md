# Parsing Redis Commands Before They Finish Arriving

When you're building a Redis proxy, every microsecond of latency matters. The typical approach is straightforward:
receive a complete command, parse it, route it. But what if you could start routing before the command finishes
arriving?

That question led us to build Eden, a RESP parser with a fundamentally different architecture than existing solutions.
Instead of waiting for complete messages, Eden lets you **parse and act on data as bytes arrive over the wire**.

## The Problem With Complete-Message Parsing

Consider a Redis command like `SET mykey <1MB value>`. With traditional parsing:

```
Time 0ms:    First bytes arrive
Time 5ms:    More bytes buffering...
Time 10ms:   Still buffering...
Time 15ms:   Complete message received
Time 15.1ms: Parse command → "SET"
Time 15.2ms: Parse key → "mykey"  
Time 15.3ms: Route to correct shard
Time 15.4ms: Begin forwarding to upstream
```

You know it's a SET command after the first few bytes. You know the key after a few more. But you're forced to wait for
the entire 1MB value before you can act on any of that information.

## What If Parsing Was Incremental?

With Eden's streaming architecture:

```
Time 0ms:    First bytes arrive
Time 0.1ms:  Parse command type → "SET" (start preparing route)
Time 0.2ms:  Parse key → "mykey" (determine target shard)
Time 0.3ms:  Open connection to upstream, begin forwarding
Time 5ms:    Continue forwarding value bytes as they arrive...
Time 15ms:   Final bytes forwarded, command complete
```

The routing decision happens in microseconds, not milliseconds. Value bytes flow through the proxy without buffering.
Memory usage stays constant regardless of value size.

## How Eden Enables This

Eden's core abstraction is the **streaming reader**. When you parse a bulk string, you don't get the bytes—you get a
reader that yields bytes as they become available:

```rust
let mut array = Array::parse( & stream).await?;

// First element: command name
let cmd_reader = array.next().await?;
let cmd = read_simple_string(cmd_reader).await?;

// We now know it's a SET - start routing logic
let route = match cmd.as_slice() {
b"SET" | b"GET" | b"DEL" => Route::ByKey,
b"MGET" | b"MSET" => Route::MultiKey,
b"PING" | b"INFO" => Route::AnyNode,
_ => Route::Primary,
};

// Second element: key (for key-based routing)  
if route == Route::ByKey {
let key_reader = array.next().await?;
let key = read_bulk_string(key_reader).await ?;
let shard = hash_slot( & key);

// Connection established, ready to forward
let upstream = pool.get_connection(shard).await ?;

// Forward remaining elements (the value) as they stream in
while let Some(element) = array.next().await ? {
forward_element( & upstream, element).await ?;
}
}
```

The key insight: **you don't wait for elements you haven't asked for yet**. Calling `array.next()` gives you the next
element as soon as its header arrives, even if the rest of the array is still in flight.

## The Architecture Difference

Existing RESP parsers like `redis-protocol` are designed for **frame extraction**. You provide a complete buffer, they
return parsed frames:

```rust
// redis-protocol: needs complete message first
let frame = decode( & complete_buffer) ?;
match frame {
Frame::Array(elements) => {
// All elements already in memory
}
}
```

This is fast and simple when you have complete data. But it fundamentally can't support incremental processing—the API
requires the complete message upfront.

Eden inverts this. Instead of "buffer then parse," it's "parse as you buffer":

```rust
// Eden: parse incrementally as data arrives
let mut array = Array::parse( & stream).await?;
// Array header parsed, elements not yet read

let first = array.next().await?;
// First element parsed (may await more network data)

let second = array.next().await?;
// Second element parsed, third+ not yet touched
```

Each `.await` point can yield to other work while waiting for network data. You process what you have while the rest
arrives.

## Dual Sync/Async APIs

Not every use case needs streaming. When you do have a complete buffer—after receiving a full response from Redis, for
example—async overhead is pure waste.

Eden provides both modes:

```rust
// Complete buffer? Use sync for zero async overhead
let value = Integer::parse_sync( & buffer_stream) ?;

// Streaming from socket? Use async
let value = Integer::parse( & socket_stream).await?;
```

Both APIs use the same parser logic. The sync path simply never yields, giving you maximum performance when streaming
isn't needed.

## Performance Results

We benchmarked Eden against `redis-protocol` to understand the real-world impact of our streaming architecture.

### Command Routing: The Proxy Use Case

In a Redis proxy, the critical path is: parse command name → parse key → route to shard. Everything after that (the
value) can stream through. We benchmarked exactly this operation:

| Command                       | Eden    | redis-protocol | Speedup         |
|-------------------------------|---------|----------------|-----------------|
| `GET k`                       | 10.7 ns | 55 ns          | **5.2x faster** |
| `GET user:profile:12345`      | 13.3 ns | 83 ns          | **6.3x faster** |
| `SET key <10KB value>`        | 10.8 ns | 94 ns          | **8.7x faster** |
| `HSET hash f1 v1 f2 v2 f3 v3` | 10.5 ns | 221 ns         | **21x faster**  |

The pattern is striking: **Eden's time is constant regardless of command size**. Extracting the command name and key
from a `SET` with a 10KB value takes the same 10.8 nanoseconds as a tiny `GET`.

redis-protocol, by contrast, must parse the entire frame structure before you can access any element. That 10KB value
you don't need yet? It still gets parsed, validated, and indexed. Those six fields in the HSET? All parsed before you
can read the command name.

This is the architectural difference in action. Eden stops parsing when you stop reading. redis-protocol parses
everything upfront because its API returns complete frames.

### Why Constant-Time Routing Matters

Consider a proxy handling mixed traffic:

- **Small commands** (`GET key`, `INCR counter`): Both parsers are fast, Eden ~5x faster
- **Medium commands** (`SET key <1KB>`): Eden maintains 10ns, redis-protocol climbs to ~100ns
- **Large commands** (`SET key <100KB>`): Eden still 10ns, redis-protocol scales linearly

In production, you don't control your traffic mix. A burst of large SET commands shouldn't slow down routing for the
small GETs queued behind them. With Eden, it doesn't—every command routes in the same ~10 nanoseconds regardless of
payload size.

### Pipeline Processing

We also benchmarked processing 100 pipelined commands:

| Operation             | Eden    | redis-protocol | Result                     |
|-----------------------|---------|----------------|----------------------------|
| Extract all keys      | 7.2 µs  | 9.9 µs         | **Eden 1.4x faster**       |
| Route by command type | 14.0 µs | 9.0 µs         | redis-protocol 1.5x faster |

The routing benchmark shows redis-protocol ahead due to our current implementation creating per-command stream state.
The key extraction benchmark—closer to real proxy workloads—shows Eden's advantage when doing targeted field access
across many commands.

### Type Parsing (Complete Buffers)

For parsing complete RESP types from buffers:

| Operation            | Eden    | redis-protocol | Result               |
|----------------------|---------|----------------|----------------------|
| Simple String        | 2.6 ns  | 7.9 ns         | **Eden 3x faster**   |
| Integer              | 8.8 ns  | 12.0 ns        | **Eden 1.4x faster** |
| Array (3 elements)   | 20.9 ns | 55.7 ns        | **Eden 2.7x faster** |
| Array (100 elements) | 0.91 µs | 1.50 µs        | **Eden 1.6x faster** |

### Large Payloads

| Operation       | Eden    | redis-protocol | Result                     |
|-----------------|---------|----------------|----------------------------|
| Bulk String 1KB | 25.3 ns | 16.1 ns        | redis-protocol 1.6x faster |
| Bulk String 1MB | 30.5 ns | 18.3 ns        | redis-protocol 1.7x faster |

redis-protocol wins on large bulk strings where minimal parsing overhead matters most. This is expected—their frame
extraction is optimized for exactly this case.

## When Streaming Parsing Matters

The streaming architecture pays off in specific scenarios:

**Request routing**: Know where to route a command (by parsing command name and key) before the full payload arrives. A
`SET key <10MB>` routes in 10 nanoseconds, not microseconds.

**Large values**: A 10MB `GET` response can start flowing to the client immediately. No need to buffer the entire value
before forwarding.

**Pipelined commands**: Parse and route the first command while subsequent commands are still arriving. Better pipeline
utilization.

**Multi-key commands**: `MGET key1 key2 ... key1000`—start resolving keys and preparing fanout before all keys arrive.

**Memory efficiency**: Never buffer more than necessary. A proxy handling 10,000 concurrent large-value requests doesn't
need 10GB of buffers.

## The Tradeoff

Our streaming readers carry overhead that pure frame extraction doesn't have. Each reader tracks its position, manages
state, and supports incremental consumption. For bulk data you're just passing through, that's unnecessary work.

We made this tradeoff intentionally. The command routing benchmarks show why: the operations that happen on the critical
path (parse command, parse key, route) are 5-21x faster with Eden. The operations where redis-protocol wins (bulk string
throughput) happen after routing, where streaming often matters more than raw parsing speed anyway.

## Conclusion

Traditional parsers answer: "What does this message contain?"

Eden answers: "What does this message contain *so far*?"

That shift—from complete-message to incremental parsing—required rethinking the parser architecture from the ground up.
The result is a parser that enables patterns that weren't possible before: routing in constant time regardless of
payload size, streaming large values without buffering, and processing pipelined commands in parallel with receiving
them.

The fastest proxy isn't the one that parses fastest. It's the one that starts working soonest.

---

*Eden is part of our Redis proxy infrastructure, designed for high-throughput Redis protocol handling where latency and
memory efficiency matter.*