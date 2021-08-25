/* How many sessions are there? */
SELECT
  COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitid AS string))) AS sessions
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`



/* How many sessions does each visitor create? */
SELECT
  fullVisitorId, COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitid AS string))) as total_session
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
GROUP BY fullVisitorId

/* How much time does it take on average to reach the order_confirmation screen per
session (in minutes)? */

WITH
  DENORM_TABLE AS (
  SELECT
    fullvisitorid,
    visitNumber,
    visitId,
    visitStartTime,
    time,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 11) AS screen
  FROM
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga,
    UNNEST(ga.hit) ),
  order_confirmation AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY fullvisitorid, visitId, visitStartTime ORDER BY time ASC) AS rank
  FROM
    DENORM_TABLE
  WHERE
    screen = 'order_confirmation' )
SELECT
  ROUND(AVG(time / 60000)) AS avg_time_mins
FROM
  order_confirmation
WHERE
  rank = 1



  /*
By using the GoogleAnalyticsSample data and BackendDataSample tables, analyse
how often users tend to change their location in the beginning of their journey (screens
like home and listing) versus in checkout and on order placement and demonstrate the
the deviation between earlier and later inputs (if any) in terms of coordinates change.
*/

WITH
  -- This Temporal Table Structures the Base table and extract the field values in the custom dimention column
  DENORM_TABLE AS (
  SELECT
    fullvisitorid,
    visitId,
    visitStartTime,
    date,
    eventAction,
    transactionid,
    time,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 11) AS screen,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 15) AS locationCountry,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 16) AS locationCity,
    SAFE_CAST((
      SELECT
        value
      FROM
        UNNEST(customDimensions)
      WHERE
        INDEX = 18) AS FLOAT64) AS locationLon,
    SAFE_CAST((
      SELECT
        value
      FROM
        UNNEST(customDimensions)
      WHERE
        INDEX = 19) AS FLOAT64) AS locationLat
  FROM
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga,
    UNNEST(ga.hit)),
  -- This Table Creates the Journey
  FUNNEL AS (
  SELECT
    * EXCEPT(screen),
    CASE
      WHEN eventAction IN ( 'address.submitted', 'address_update.submitted') THEN LAG(CONCAT(coalesce(locationLat),',',coalesce(locationLon))) OVER(PARTITION BY fullvisitorid, visitStartTime ORDER BY time ASC)
  END
    prev_loc,
    CASE
      WHEN screen IN ('home', 'shop_list') THEN 'Home'
      WHEN screen IN ('checkout') THEN 'Checkout'
      WHEN screen IN ('order_confirmation') THEN 'Order Placement'
  END
    AS screen
  FROM
    denorm_table
  WHERE
    screen IN ('order_confirmation',
      'home',
      'shop_list',
      'checkout')
    AND (locationLat IS NOT NULL
      OR locationLon IS NOT NULL) ),
  ADDRESS_CHANGE_FUNNEL AS (
  SELECT
    *,
    CASE
      WHEN eventAction IN ('address.submitted', 'address_update.submitted') AND CONCAT(coalesce(locationLat),',',coalesce(locationLon)) != prev_loc THEN 1
    ELSE
    0
  END
    AS change_flag
  FROM
    FUNNEL )
  -- Aggregating count of Address Change
SELECT
  screen,
  COUNT(DISTINCT CONCAT(fullvisitorid, visitStartTime)) AS sessions,
  SUM(change_flag) AS times,
  COUNT(1) AS home_event_count
FROM
  ADDRESS_CHANGE_FUNNEL
GROUP BY
  screen




/*
Then, using the BackendDataSample table, see if those customers who changed their
address ended placing orders and if those orders were delivered successfully, if so, did
they match their destination.
*/

WITH
  -- This Temporal Table Structures the Base table and extract the field values in the custom dimention column
  DENORM_TABLE AS (
  SELECT
    fullvisitorid,
    CONCAT(fullvisitorid, CAST(visitStartTime AS string)) AS SESSION,
    visitId,
    visitStartTime,
    date,
    eventAction,
    transactionid,
    time,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 11) AS screen,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 15) AS locationCountry,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 16) AS locationCity,
    SAFE_CAST((
      SELECT
        value
      FROM
        UNNEST(customDimensions)
      WHERE
        INDEX = 18) AS FLOAT64) AS locationLon,
    SAFE_CAST((
      SELECT
        value
      FROM
        UNNEST(customDimensions)
      WHERE
        INDEX = 19) AS FLOAT64) AS locationLat
  FROM
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga,
    UNNEST(ga.hit)),
  -- This Table Creates the Journey
  FUNNEL AS (
  SELECT
    * EXCEPT(screen),
    CASE
      WHEN eventAction IN ( 'address.submitted', 'address_update.submitted') THEN LAG(CONCAT(coalesce(locationLat),',',coalesce(locationLon))) OVER(PARTITION BY fullvisitorid, visitStartTime ORDER BY time ASC)
  END
    prev_loc,
    CASE
      WHEN screen IN ('home', 'shop_list') THEN 'Home'
      WHEN screen IN ('checkout') THEN 'Checkout'
      WHEN screen IN ('order_confirmation') THEN 'Order Placement'
  END
    AS screen
  FROM
    denorm_table
  WHERE
    screen IN ('order_confirmation',
      'home',
      'shop_list',
      'checkout')
    AND (locationLat IS NOT NULL
      OR locationLon IS NOT NULL)),
  ADDRESS_CHANGE_FUNNEL AS (
  SELECT
    *,
    CASE
      WHEN eventAction IN ('address.submitted', 'address_update.submitted') AND CONCAT(coalesce(locationLat),',',coalesce(locationLon)) != prev_loc THEN 1
    ELSE
    0
  END
    AS change_flag
  FROM
    FUNNEL ),
  -- Aggregating count of Address Change
  FUNNEL_ADDRESS_CHANGE_AGG AS (
  SELECT
    SESSION,
    SUM(change_flag) AS times
  FROM
    ADDRESS_CHANGE_FUNNEL
  GROUP BY
    SESSION)
SELECT
  hcg.*,
  txn.locationLat,
  txn.locationLon,
  transactionid,
  CASE
    WHEN times = 0 THEN FALSE
  ELSE
  TRUE
END
  AS address_changed,
  CASE
    WHEN tr.frontendOrderId IS NULL THEN FALSE
  ELSE
  TRUE
END
  AS order_placed,
  CASE
    WHEN tr.geopointDropoff IS NULL THEN FALSE
  ELSE
  TRUE
END
  AS order_delivered
FROM
  FUNNEL_ADDRESS_CHANGE_AGG hcg
LEFT JOIN (
  SELECT
    SESSION,
    LOCATIONLON,
    LOCATIONLAT,
    TRANSACTIONID,
    MAX(TIME) AS TIME
  FROM
    denorm_table
  WHERE
    transactionid IS NOT NULL
  GROUP BY
    SESSION,
    LOCATIONLON,
    LOCATIONLAT,
    TRANSACTIONID ) txn
ON
  TXN.SESSION = HCG.SESSION
LEFT JOIN
  `dhh-analytics-hiringspace.BackendDataSample.transactionalData` tr
ON
  txn.transactionid = tr.frontendOrderId
  AND tr.declinereason_code IS NULL