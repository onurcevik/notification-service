CREATE TYPE channel AS ENUM ('sms', 'email', 'push');
CREATE TYPE priority AS ENUM ('high', 'normal', 'low');
CREATE TYPE notif_status AS ENUM (
    'pending',
    'processing',
    'delivered',
    'failed',
    'cancelled',
    'scheduled'
);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE notifications (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  TEXT,
    batch_id         UUID,
    channel          channel      NOT NULL,
    priority         priority     NOT NULL DEFAULT 'normal',
    status           notif_status NOT NULL DEFAULT 'pending',
    recipient        TEXT         NOT NULL,
    content          TEXT         NOT NULL,
    scheduled_at     TIMESTAMPTZ,
    attempt_count    INT          NOT NULL DEFAULT 0,
    provider_msg_id  TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_notifications_idempotency
    ON notifications (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_notifications_status_channel
    ON notifications (status, channel, created_at DESC);

CREATE INDEX idx_notifications_batch_id
    ON notifications (batch_id)
    WHERE batch_id IS NOT NULL;

CREATE INDEX idx_notifications_scheduled
    ON notifications (scheduled_at)
    WHERE status = 'scheduled';

CREATE TRIGGER notifications_updated_at
    BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();