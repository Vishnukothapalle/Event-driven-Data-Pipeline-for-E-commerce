-- Check current version
SELECT order_id, order_status, valid_from, valid_to, is_current, order_sk
FROM ecommerce_Silver_Layer.scd2_dim_order 
WHERE order_id = '799dc0fd216e2a8e571a2684ddb6f940';

-- Update just ONE row in staging to simulate status change
UPDATE ecommerce_Silver_Layer.staging_dim_order
SET 
  order_status = 'delivered',
  load_timestamp = '2025-11-11 23:45:00.123456+00:00'   -- new load time (today!)
WHERE order_id = '799dc0fd216e2a8e571a2684ddb6f940';

MERGE INTO ecommerce_Silver_Layer.scd2_dim_order AS target
USING (
  SELECT
    order_id,
    customer_id,
    order_status,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_purchase_timestamp)          AS order_purchase_timestamp,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_approved_at)                AS order_approved_at,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_delivered_carrier_date)     AS order_delivered_carrier_date,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_delivered_customer_date)    AS order_delivered_customer_date,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_estimated_delivery_date)    AS order_estimated_delivery_date,
    TIMESTAMP(REPLACE(load_timestamp, ' ', 'T'))                              AS load_timestamp,
    TO_HEX(MD5(CONCAT(
      COALESCE(order_status, ''),
      COALESCE(order_purchase_timestamp, ''),
      COALESCE(order_approved_at, ''),
      COALESCE(order_delivered_carrier_date, ''),
      COALESCE(order_delivered_customer_date, ''),
      COALESCE(order_estimated_delivery_date, '')
    ))) AS row_hash,
    ROW_NUMBER() OVER (ORDER BY order_id) + 
      COALESCE((SELECT MAX(order_sk) FROM ecommerce_Silver_Layer.scd2_dim_order), 0) AS new_order_sk
  FROM ecommerce_Silver_Layer.staging_dim_order
  WHERE load_timestamp != 'load_timestamp'   -- skip header if any
) AS src
ON target.order_id = src.order_id AND target.is_current = TRUE

WHEN MATCHED AND target.row_hash != src.row_hash THEN
  UPDATE SET 
    target.valid_to = src.load_timestamp,
    target.is_current = FALSE,
    target.row_hash = src.row_hash

WHEN NOT MATCHED BY TARGET THEN
  INSERT (order_sk, order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
          order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
          load_timestamp, row_hash, valid_from, valid_to, is_current)
  VALUES (src.new_order_sk, src.order_id, src.customer_id, src.order_status, src.order_purchase_timestamp,
          src.order_approved_at, src.order_delivered_carrier_date, src.order_delivered_customer_date,
          src.order_estimated_delivery_date, src.load_timestamp, src.row_hash,
          src.load_timestamp, TIMESTAMP('9999-12-31 23:59:59'), TRUE);

-- FULL HISTORY OF THIS ORDER — THIS IS WHAT YOUR BOSS WILL LOVE
SELECT 
  order_id,
  order_status,
  valid_from,
  valid_to,
  is_current,
  order_sk
FROM ecommerce_Silver_Layer.scd2_dim_order 
WHERE order_id = '799dc0fd216e2a8e571a2684ddb6f940'
ORDER BY valid_from;





-- Drop the broken table (safe – your data is in staging_dim_order)
DROP TABLE IF EXISTS ecommerce_Silver_Layer.scd2_dim_order;

-- Create it with CORRECT data types (TIMESTAMP for all date columns)
CREATE TABLE ecommerce_Silver_Layer.scd2_dim_order (
    order_sk                      INT64     NOT NULL,
    order_id                      STRING    NOT NULL,
    customer_id                   STRING,
    order_status                  STRING,
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    load_timestamp                TIMESTAMP,
    row_hash                      STRING,
    valid_from                    TIMESTAMP NOT NULL,
    valid_to                      TIMESTAMP NOT NULL,
    is_current                    BOOL      NOT NULL
)
PARTITION BY DATE(valid_from)
CLUSTER BY order_id;

MERGE INTO ecommerce_Silver_Layer.scd2_dim_order AS target
USING (
  SELECT
    order_id,
    customer_id,
    order_status,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_purchase_timestamp)          AS order_purchase_timestamp,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_approved_at)                AS order_approved_at,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_delivered_carrier_date)     AS order_delivered_carrier_date,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_delivered_customer_date)    AS order_delivered_customer_date,
    SAFE.PARSE_TIMESTAMP('%d-%m-%Y %H:%M', order_estimated_delivery_date)    AS order_estimated_delivery_date,
    
    -- THIS IS THE FINAL BULLETPROOF LINE
    TIMESTAMP(REPLACE(load_timestamp, ' ', 'T')) AS load_timestamp,
    
    TO_HEX(MD5(CONCAT(
      COALESCE(order_status, ''),
      COALESCE(order_purchase_timestamp, ''),
      COALESCE(order_approved_at, ''),
      COALESCE(order_delivered_carrier_date, ''),
      COALESCE(order_delivered_customer_date, ''),
      COALESCE(order_estimated_delivery_date, '')
    ))) AS row_hash,

    ROW_NUMBER() OVER (ORDER BY order_id) + 
      COALESCE((SELECT MAX(order_sk) FROM ecommerce_Silver_Layer.scd2_dim_order), 0) AS new_order_sk

  FROM ecommerce_Silver_Layer.staging_dim_order
  
  -- THIS LINE REMOVES THE HEADER ROW THAT SAYS 'load_timestamp'
  WHERE load_timestamp != 'load_timestamp'
    AND load_timestamp IS NOT NULL
    AND load_timestamp NOT LIKE '%order_id%'
) AS src
ON target.order_id = src.order_id AND target.is_current = TRUE

WHEN MATCHED AND target.row_hash != src.row_hash THEN
  UPDATE SET target.valid_to = src.load_timestamp, target.is_current = FALSE, target.row_hash = src.row_hash

WHEN NOT MATCHED BY TARGET THEN
  INSERT (order_sk, order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
          order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
          load_timestamp, row_hash, valid_from, valid_to, is_current)
  VALUES (src.new_order_sk, src.order_id, src.customer_id, src.order_status, src.order_purchase_timestamp,
          src.order_approved_at, src.order_delivered_carrier_date, src.order_delivered_customer_date,
          src.order_estimated_delivery_date, src.load_timestamp, src.row_hash,
          src.load_timestamp, TIMESTAMP('9999-12-31 23:59:59'), TRUE);

  SELECT 
  order_id, 
  order_status, 
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', order_purchase_timestamp) AS purchase_date,
  is_current 
FROM ecommerce_Silver_Layer.scd2_dim_order 
WHERE order_id = '799dc0fd216e2a8e571a2684ddb6f940';