--Battery Materialized view 
	--1.Battery health overview
CREATE MATERIALIZED VIEW IF NOT EXISTS battery_health_mv 
ENGINE = MergeTree() 
ORDER BY (time, battery_id)
POPULATE AS
SELECT 
    time, 
    battery_id, 
    station_id, 
    temperature_bms_pcb, 
    temperature_internal, 
    temperature_cell
FROM battery_events;


	--2.Battery performance overview
CREATE MATERIALIZED VIEW IF NOT EXISTS battery_performance_mv 
ENGINE = MergeTree() 
ORDER BY (time, battery_id)
POPULATE AS
SELECT 
    time, 
    battery_id, 
    soc, 
    current_flow, 
    speed 
FROM battery_events;



---Mysql Customer, orders,battery_swaps materialized views
	--1.Station Swap Overview (Swaps Count Per Station Name)
CREATE MATERIALIZED VIEW IF NOT EXISTS station_swap_summary_mv
ENGINE = SummingMergeTree()
ORDER BY station_name
POPULATE AS
SELECT 
    station_name, 
    COUNT(swap_id) AS total_swaps
FROM battery_swaps
GROUP BY station_name;


	--2.Daily Order Revenue & Order Status Overview
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_order_metrics_mv
ENGINE = SummingMergeTree()
ORDER BY order_date
POPULATE AS
SELECT 
    toDate(order_date) AS order_date, 
    order_status, 
    SUM(total_amount) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM orders
GROUP BY order_date, order_status;


