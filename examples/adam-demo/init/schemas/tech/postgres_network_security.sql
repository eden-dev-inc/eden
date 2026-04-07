-- PostgreSQL schema for ADAM Demo — Tech vertical: Network Security silo
-- Source: UNSW-NB15 dataset (~2.5M network flows)
-- Simulates: SecOps team's intrusion detection system

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Raw network flow records from UNSW-NB15
CREATE TABLE IF NOT EXISTS network_flows (
    flow_id        BIGSERIAL PRIMARY KEY,
    srcip          VARCHAR(45),
    sport          INTEGER,
    dstip          VARCHAR(45),
    dsport         INTEGER,
    proto          VARCHAR(16),
    state          VARCHAR(16),
    dur            DOUBLE PRECISION,       -- duration
    sbytes         BIGINT,                 -- source bytes
    dbytes         BIGINT,                 -- dest bytes
    sttl           INTEGER,                -- source TTL
    dttl           INTEGER,                -- dest TTL
    sloss          INTEGER,                -- source packets retransmitted
    dloss          INTEGER,                -- dest packets retransmitted
    service        VARCHAR(32),            -- http, ftp, smtp, ssh, dns, etc.
    sload          DOUBLE PRECISION,       -- source bits per second
    dload          DOUBLE PRECISION,       -- dest bits per second
    spkts          INTEGER,                -- source packets
    dpkts          INTEGER,                -- dest packets
    swin           INTEGER,                -- source TCP window
    dwin           INTEGER,                -- dest TCP window
    stcpb          BIGINT,                 -- source TCP base seq
    dtcpb          BIGINT,                 -- dest TCP base seq
    smeansz        INTEGER,                -- mean source packet size
    dmeansz        INTEGER,                -- mean dest packet size
    trans_depth    INTEGER,                -- pipelined depth into connection
    res_bdy_len    INTEGER,                -- response body length
    sjit           DOUBLE PRECISION,       -- source jitter
    djit           DOUBLE PRECISION,       -- dest jitter
    sinpkt         DOUBLE PRECISION,       -- source inter-packet arrival time
    dinpkt         DOUBLE PRECISION,       -- dest inter-packet arrival time
    tcprtt         DOUBLE PRECISION,       -- TCP round trip time
    synack         DOUBLE PRECISION,       -- SYN to SYN-ACK time
    ackdat         DOUBLE PRECISION,       -- SYN-ACK to ACK time
    is_sm_ips_ports BOOLEAN,               -- src=dst IP and port
    ct_state_ttl   INTEGER,                -- connections with same state in last 100
    ct_flw_http_mthd INTEGER,              -- HTTP methods in last 100
    is_ftp_login   BOOLEAN,               -- FTP session with login
    ct_ftp_cmd     INTEGER,                -- FTP commands in session
    ct_srv_src     INTEGER,                -- connections to same service from src
    ct_srv_dst     INTEGER,                -- connections to same service from dst
    ct_dst_ltm     INTEGER,                -- connections to same dst in last 100
    ct_src_ltm     INTEGER,                -- connections from same src in last 100
    ct_src_dport_ltm INTEGER,              -- connections from src to same dst port
    ct_dst_sport_ltm INTEGER,              -- connections to dst from same src port
    ct_dst_src_ltm INTEGER,                -- connections between same src-dst pair
    attack_cat     VARCHAR(32),            -- Normal, Fuzzers, Analysis, Backdoor, DoS, Exploits, Generic, Reconnaissance, Shellcode, Worms
    label          INTEGER                 -- 0=normal, 1=attack
);

-- Security alerts derived from flow analysis
CREATE TABLE IF NOT EXISTS security_alerts (
    alert_id       BIGSERIAL PRIMARY KEY,
    flow_id        BIGINT REFERENCES network_flows(flow_id),
    alert_type     VARCHAR(32) NOT NULL,   -- intrusion, anomaly, policy_violation, scan
    severity       VARCHAR(16) NOT NULL,   -- low, medium, high, critical
    attack_cat     VARCHAR(32),
    srcip          VARCHAR(45),
    dstip          VARCHAR(45),
    description    TEXT,
    status         VARCHAR(16) NOT NULL DEFAULT 'open',  -- open, investigating, resolved, false_positive
    created_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for common security queries
CREATE INDEX IF NOT EXISTS idx_flows_srcip       ON network_flows(srcip);
CREATE INDEX IF NOT EXISTS idx_flows_dstip       ON network_flows(dstip);
CREATE INDEX IF NOT EXISTS idx_flows_proto       ON network_flows(proto);
CREATE INDEX IF NOT EXISTS idx_flows_service     ON network_flows(service);
CREATE INDEX IF NOT EXISTS idx_flows_attack      ON network_flows(attack_cat);
CREATE INDEX IF NOT EXISTS idx_flows_label       ON network_flows(label);
CREATE INDEX IF NOT EXISTS idx_flows_state       ON network_flows(state);

CREATE INDEX IF NOT EXISTS idx_alerts_type       ON security_alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_severity   ON security_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_status     ON security_alerts(status);
CREATE INDEX IF NOT EXISTS idx_alerts_srcip      ON security_alerts(srcip);
