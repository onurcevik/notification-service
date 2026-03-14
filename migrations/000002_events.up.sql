CREATE TABLE events (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id  UUID        NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    published        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_unpublished
    ON events (created_at ASC)
    WHERE published = FALSE;