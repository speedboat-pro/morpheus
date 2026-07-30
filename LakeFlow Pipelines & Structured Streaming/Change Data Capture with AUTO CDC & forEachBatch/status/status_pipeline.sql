--------------------------------------------------------
-- Status PIPELINE
--------------------------------------------------------

--------------------------------------------------------
-- A. Bronze Table Creation
--------------------------------------------------------
-- Code below creates the bronze table **status_bronze_demo12** incrementally ingesting JSON files
-- The code below ingests JSON files located in your `/Volumes/dbacademy/ops/your-lab-user/status/` volume, using the `source` DLT configuration parameter to point to the base path `/Volumes/dbacademy/ops/your-lab-user/`.
--------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.status_bronze_demo12
  COMMENT "Ingest raw JSON order status files from cloud storage"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false  -- prevent full table refreshes on the bronze table
  )
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}/status",
  format => "json"
);


--------------------------------------------------------
-- B. Bronze -> Silver
--------------------------------------------------------
-- The code below performs a simple transformation on the date field and selects only the necessary columns
-- for the silver streaming table **status_silver_demo12**.
-- We’re also adding a comment and table properties to document the table for production use,
-- along with pipeline expectations to enforce data quality on the streaming table.
--------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_2_silver.status_silver_demo12
(
  -- Drop rows if order_status_timestamp is not valid
  CONSTRAINT valid_timestamp
  EXPECT (order_status_timestamp > "2021-12-25")
  ON VIOLATION DROP ROW,

  -- Warn if order_status is not in the following
  CONSTRAINT valid_order_status
  EXPECT (
    order_status IN (
      'on the way',
      'canceled',
      'return canceled',
      'delivered',
      'return processed',
      'placed',
      'preparing'
    )
  )
)
COMMENT "Order with each status and timestamp"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  order_id,
  order_status,
  timestamp(status_timestamp) AS order_status_timestamp
FROM STREAM sdp_1_bronze.status_bronze_demo12;



--------------------------------------------------------
-- C. Use a Materialized View to Join Two Streaming Tables
--------------------------------------------------------
-- One way to join two streaming tables in Spark Declarative Pipelines is by creating a materialized view
-- that performs the join.
-- This approach takes all rows from each streaming table and executes a full inner join operation
-- and incorporates optimizations where applicable.
--------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.full_order_info_gold_demo12
  COMMENT "Joining the orders and order status silver tables to view all orders with each individual status per order"
  TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  orders.order_id,
  orders.order_timestamp,
  status.order_status,
  status.order_status_timestamp
-- Notice that the STREAM keyword was not used when referencing the streaming tables to create the MV
FROM sdp_2_silver.status_silver_demo12 status
INNER JOIN sdp_2_silver.orders_silver_demo12 orders
  ON orders.order_id = status.order_id;




--------------------------------------------------------
-- D. Create Gold Materialized Views for Cancelled and Delivered Orders
-- The code below will create two gold materialized views using the joined data from above (orders and status):
--------------------------------------------------------

-- CANCELLED ORDERS MV
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.cancelled_orders_gold_demo12
  COMMENT "All cancelled orders"
  TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  order_id,
  order_timestamp,
  order_status,
  order_status_timestamp,
  datediff(DAY, order_timestamp, order_status_timestamp) AS days_to_cancel  -- calculate days to cancel
FROM sdp_3_gold.full_order_info_gold_demo12
WHERE order_status = 'canceled';

-- DELIVERED ORDERS MV
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.delivered_orders_gold_demo12
  COMMENT "All delivered orders"
  TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  order_id,
  order_timestamp,
  order_status,
  order_status_timestamp,
  datediff(DAY, order_timestamp, order_status_timestamp) AS days_to_delivery  -- calculate days to deliver
FROM sdp_3_gold.full_order_info_gold_demo12
WHERE order_status = 'delivered';
