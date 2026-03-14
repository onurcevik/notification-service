DROP TRIGGER IF EXISTS notifications_updated_at ON notifications;
DROP TABLE IF EXISTS notifications;
DROP FUNCTION IF EXISTS set_updated_at();
DROP TYPE IF EXISTS notif_status;
DROP TYPE IF EXISTS priority;
DROP TYPE IF EXISTS channel;