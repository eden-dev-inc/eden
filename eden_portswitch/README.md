# Eden PortSwitch

Linux port redirection tool for Eden database migrations. Redirects application traffic through the Eden gateway without
modifying application code, while preventing redirect loops using UID-based exclusion.

## Installation

```bash
cargo build --release
sudo cp target/release/eden-portswitch /usr/local/bin/
```

## The Problem

Eden proxies database connections for migration. The naive approach creates an infinite loop:

```
App → :6378 → [redirect] → Eden(:6366) → :6378 → [redirect] → Eden → ∞
```

Eden's own connection to Redis also gets redirected back to itself.

## The Solution

Linux iptables can exclude traffic by UID. Run Eden as a dedicated user, and exclude that user from the redirect:

```
App (any user) → :6378 → [redirect] → Eden(:6366) ✓
Eden (user: eden) → :6378 → [NOT redirected] → Redis ✓
```

## Usage

```bash
# Preflight: verify everything is ready
sudo eden-portswitch preflight --eden-port 6366 --redis-port 6378 --eden-user eden

# Add redirect (exclude Eden's traffic)
sudo eden-portswitch add --from 6378 --to 6366 --exclude-user eden

# List active rules
sudo eden-portswitch list

# Remove redirect
sudo eden-portswitch remove --from 6378 --to 6366 --exclude-user eden

# Clear all Eden rules
sudo eden-portswitch clear

# Dry run (show commands without executing)
eden-portswitch dry-run --from 6378 --to 6366 --exclude-user eden
```

### Options

| Flag                 | Description                                        |
|----------------------|----------------------------------------------------|
| `-f, --from`         | Source port (app's target)                         |
| `-t, --to`           | Destination port (Eden gateway)                      |
| `-e, --exclude-user` | User whose traffic is NOT redirected (Eden's user) |
| `-p, --protocol`     | Protocol: tcp (default) or udp                     |

---

## Eden Redis Migration Example

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           BEFORE                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────────┐                         ┌─────────────────────┐   │
│   │ App (user X)│ ──────── :6378 ───────▶ │       Redis         │   │
│   └─────────────┘                         │       :6378         │   │
│                                           └─────────────────────┘   │
│   ┌─────────────┐                                   ▲               │
│   │    Eden     │ ──────── :6378 ───────────────────┘               │
│   │ (user eden) │                                                   │
│   │   :6366     │                                                   │
│   └─────────────┘                                                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                           AFTER                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────────┐     iptables REDIRECT   ┌─────────────────────┐   │
│   │ App (user X)│ ─:6378──(6378→6366)───▶ │    Eden (:6366)     │   │
│   └─────────────┘                         │    (user eden)      │   │
│                                           └──────────┬──────────┘   │
│                                                      │              │
│                                                      │ :6378        │
│                                                      │ (excluded    │
│                                                      │  from        │
│                                                      │  redirect)   │
│                                                      ▼              │
│                                           ┌─────────────────────┐   │
│                                           │       Redis         │   │
│                                           │       :6378         │   │
│                                           └─────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Step-by-Step

#### 1. Create Eden user

```bash
sudo useradd -r -s /bin/false eden
```

#### 2. Start Redis (if not already running)

```bash
redis-server --port 6378
```

#### 3. Start Eden as the `eden` user

```bash
sudo -u eden eden-gateway --listen 6366 --upstream 127.0.0.1:6378
```

#### 4. Validate setup

```bash
# Test Eden gateway works
redis-cli -p 6366 PING
# Should return: PONG

# Run preflight checks
sudo eden-portswitch preflight \
    --eden-port 6366 \
    --redis-port 6378 \
    --eden-user eden
```

#### 5. Apply the redirect

```bash
# Preview first
eden-portswitch dry-run --from 6378 --to 6366 --exclude-user eden

# Apply
sudo eden-portswitch add --from 6378 --to 6366 --exclude-user eden
```

#### 6. Verify

```bash
# App's connection now goes through Eden
redis-cli -p 6378 PING
# Traffic: App → iptables → Eden(:6366) → Redis(:6378)
# Should return: PONG

# Check the rule is in place
sudo eden-portswitch list
```

### Rollback

```bash
# Remove specific rule
sudo eden-portswitch remove --from 6378 --to 6366 --exclude-user eden

# Or clear all Eden rules
sudo eden-portswitch clear
```

---

## How It Works

The tool manages iptables NAT OUTPUT chain rules:

```bash
# What gets added:
iptables -t nat -A OUTPUT \
    -p tcp \
    --dport 6378 \
    -m owner ! --uid-owner <eden-uid> \
    -j REDIRECT --to-port 6366 \
    -m comment --comment "eden-portswitch"
```

Key components:

- `-t nat -A OUTPUT`: Intercepts locally-originated outbound traffic
- `--dport 6378`: Match traffic destined for the original port
- `-m owner ! --uid-owner <uid>`: Exclude traffic from Eden's user
- `-j REDIRECT --to-port 6366`: Send to Eden gateway instead
- `--comment "eden-portswitch"`: Tag for easy identification/cleanup

---

## Requirements

- Linux with iptables
- Root privileges (for iptables manipulation)
- Eden must run as a dedicated user (for UID exclusion)

---

## Troubleshooting

### "Permission denied"

Run with `sudo`.

### "User not found"

Create the Eden user:

```bash
sudo useradd -r -s /bin/false eden
```

### Redirect not working

Check the rule exists:

```bash
sudo iptables -t nat -L OUTPUT -n -v
```

Verify traffic is hitting the rule (check packet counters).

### Still getting loops

Ensure Eden is actually running as the excluded user:

```bash
ps aux | grep eden
```

The UID in the process list must match the excluded user.

### Connection refused after redirect

Eden must be running and listening before you apply the redirect:

```bash
ss -tlnp | grep 6366
```

---

## License

MIT