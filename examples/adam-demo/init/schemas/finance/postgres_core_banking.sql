-- Finance vertical: Core Banking silo
-- Source: CiferAI/Cifer-Fraud-Detection-Dataset-AF (21M transactions)
-- PaySim-style mobile money transactions with fraud labels

CREATE TABLE IF NOT EXISTS transactions (
    txn_id         BIGSERIAL PRIMARY KEY,
    step           INTEGER,              -- unit of time (1 step = 1 hour)
    type           VARCHAR(16) NOT NULL,  -- CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER
    amount         DOUBLE PRECISION NOT NULL,
    name_orig      VARCHAR(32),           -- sender account
    oldbalance_org DOUBLE PRECISION,      -- sender balance before
    newbalance_org DOUBLE PRECISION,      -- sender balance after
    name_dest      VARCHAR(32),           -- receiver account
    oldbalance_dest DOUBLE PRECISION,     -- receiver balance before
    newbalance_dest DOUBLE PRECISION,     -- receiver balance after
    is_fraud       INTEGER NOT NULL DEFAULT 0,
    is_flagged_fraud INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_txn_type      ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_txn_fraud     ON transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_txn_flagged   ON transactions(is_flagged_fraud);
CREATE INDEX IF NOT EXISTS idx_txn_amount    ON transactions(amount);
CREATE INDEX IF NOT EXISTS idx_txn_orig      ON transactions(name_orig);
CREATE INDEX IF NOT EXISTS idx_txn_dest      ON transactions(name_dest);
CREATE INDEX IF NOT EXISTS idx_txn_step      ON transactions(step);
