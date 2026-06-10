CREATE INDEX IF NOT EXISTS agent_cards_active_name_idx
ON agent_cards (is_active, name);
