-- Application-specific tables for o4x sample app
-- (outbox and consumer_inbox are created by Dockerfile.postgres)

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status ON users(status);

-- Orders table
-- Note: user_id does not have FK constraint to allow benchmark testing with random UUIDs
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    total_price INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at DESC);

-- Inventory table
CREATE TABLE IF NOT EXISTS inventory (
    product_id VARCHAR(255) PRIMARY KEY,
    quantity INTEGER NOT NULL DEFAULT 0,
    reserved INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT check_inventory_quantity CHECK (quantity >= 0),
    CONSTRAINT check_inventory_reserved CHECK (reserved >= 0),
    CONSTRAINT check_inventory_available CHECK (quantity >= reserved)
);

CREATE INDEX idx_inventory_updated_at ON inventory(updated_at DESC);

-- Notifications table
CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    recipient VARCHAR(255) NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_notifications_status ON notifications(status);
CREATE INDEX idx_notifications_created_at ON notifications(created_at DESC);
CREATE INDEX idx_notifications_recipient ON notifications(recipient);

-- Order confirmations table (for idempotency demonstration)
CREATE TABLE IF NOT EXISTS order_confirmations (
    event_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    user_id UUID NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_order_confirmations_order_id ON order_confirmations(order_id);
CREATE INDEX idx_order_confirmations_processed_at ON order_confirmations(processed_at DESC);

-- User welcome credits table (for idempotency demonstration)
CREATE TABLE IF NOT EXISTS user_welcome_credits (
    event_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    credit_amount INTEGER NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_user_welcome_credits_user_id ON user_welcome_credits(user_id);
CREATE INDEX idx_user_welcome_credits_granted_at ON user_welcome_credits(granted_at DESC);

-- Seed some inventory data
INSERT INTO inventory (product_id, quantity, reserved, updated_at) VALUES
    ('product-001', 100, 0, clock_timestamp()),
    ('product-002', 50, 0, clock_timestamp()),
    ('product-003', 200, 0, clock_timestamp()),
    ('product-004', 10, 0, clock_timestamp()),
    ('product-005', 0, 0, clock_timestamp())
ON CONFLICT (product_id) DO NOTHING;

-- Seed benchmark users (100 users for performance testing)
INSERT INTO users (id, email, name, status, created_at, updated_at)
SELECT
    gen_random_uuid(),
    'bench-user-' || generate_series || '@example.com',
    'Benchmark User ' || generate_series,
    'active',
    clock_timestamp(),
    clock_timestamp()
FROM generate_series(1, 100)
ON CONFLICT (email) DO NOTHING;
