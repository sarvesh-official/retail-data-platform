-- runs automatically once the app starts
CREATE TABLE IF NOT EXISTS sales_orders (
    order_id        VARCHAR(50)  NOT NULL,
    category        VARCHAR(100) NOT NULL,
    sub_category    VARCHAR(100) NOT NULL,
    quantity        INTEGER      NOT NULL,
    unit_price      NUMERIC(12, 4) NOT NULL,
    total_amount    NUMERIC(14, 2) NOT NULL,
    order_tier      VARCHAR(20)  NOT NULL,
    processed_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, sub_category)
);
