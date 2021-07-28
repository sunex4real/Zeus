/* How many sessions are there? */
SELECT
  COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitid AS string))) AS sessions
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`



/* How many sessions does each visitor create? */
SELECT
  COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitid AS string))) / COUNT(DISTINCT fullvisitorid) AS sessions_per_user
FROM
  `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`


/* How much time does it take on average to reach the order_confirmation screen per
session (in minutes)? */

WITH
  DENORM_TABLE AS (
  SELECT
    fullvisitorid,
    visitNumber,
    visitId,
    visitStartTime,
    date,
    eventCategory,
    eventAction,
    screenName,
    landingScreenName,
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
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 18) AS locationLon,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 19) AS locationLat,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 25) AS orderPaymentMethod,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 40) AS userLoggedIn
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
  ROUND(SUM(time / 60000) / COUNT(DISTINCT CONCAT(fullvisitorid, CAST(visitid AS string)))) AS avg_time_mins
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
      'checkout') ),
  -- Checking for Address Change by comparing the previous record of the cordinates
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
    FUNNEL
    )
  -- Aggregating count of Address Change
  SELECT
    screen, count(distinct concat(fullvisitorid,
    visitStartTime)) as sessions,
    SUM(change_flag) AS times,
    COUNT(1) AS home_event_count
  FROM
    ADDRESS_CHANGE_FUNNEL
  GROUP BY screen




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
      'checkout') ),
  -- Checking for Address Change by comparing the previous record of the cordinates
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
    FUNNEL
    ),
  -- Aggregating count of Address Change
  FUNNEL_ADDRESS_CHANGE_AGG AS (
  SELECT
    fullvisitorid,
    visitStartTime,
    SUM(change_flag) AS times,
    COUNT(1) AS home_event_count,
    COUNT(DISTINCT screen) AS journey
  FROM
    ADDRESS_CHANGE_FUNNEL
  GROUP BY
    fullvisitorid,
    visitStartTime
  HAVING
    journey = 3 )
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
    DISTINCT fullvisitorid,
    visitStartTime,
    transactionid,
    locationLat,
    locationLon
  FROM
    denorm_table
  WHERE
    transactionid IS NOT NULL ) txn
ON
  CONCAT(txn.fullvisitorid, txn.visitStartTime) = CONCAT(hcg.fullvisitorid, hcg.visitStartTime)
LEFT JOIN
  `dhh-analytics-hiringspace.BackendDataSample.transactionalData` tr
ON
  txn.transactionid = tr.frontendOrderId
  AND tr.declinereason_code IS NULL