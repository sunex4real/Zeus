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
    time,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 11) AS screen,
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
        INDEX = 19) AS FLOAT64) AS locationLat,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 36) AS transactionid,
  FROM
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga,
    UNNEST(ga.hit)),
  --This temp table filters out cordinates with null values
  clean_data_filtering_null_location AS (
  SELECT
    *
  FROM
    DENORM_TABLE
  WHERE
    (locationLat IS NOT NULL
      OR locationLon IS NOT NULL)),
  --- Creating the user journey by ranking with the time
  user_journey AS (
  SELECT
    * EXCEPT(screen),
    CASE
      WHEN screen IN ('home', 'shop_list') THEN 'Home'
      WHEN screen IN ('checkout') THEN 'Checkout'
      WHEN screen IN ('order_confirmation') THEN 'Order Confirmation'
  END
    AS screen,
    RANK() OVER (PARTITION BY fullvisitorid, visitId, visitStartTime ORDER BY time ASC) AS step
  FROM
    clean_data_filtering_null_location
  WHERE
    screen IN ('order_confirmation',
      'home',
      'shop_list',
      'checkout'))
SELECT
  a.fullvisitorid,
  a.visitId,
  a.screen,
  a.visitStartTime,
  CASE
    WHEN CONCAT(a.locationLat, a.locationlon) != CONCAT(b.locationLat, b.locationlon) THEN 'Changed'
  ELSE
  'Unchanged'
END
  AS change_flag,
  a.transactionid
FROM
  user_journey a
JOIN
  user_journey b
ON
  CONCAT(b.fullvisitorid, CAST(b.visitStartTime AS string)) = CONCAT(a.fullvisitorid, CAST(b.visitStartTime AS string))
  AND a.step + 1 = b.step

  /*
  Analyze if those customers who changed their
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
    time,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 11) AS screen,
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
        INDEX = 19) AS FLOAT64) AS locationLat,
    (
    SELECT
      value
    FROM
      UNNEST(customDimensions)
    WHERE
      INDEX = 36) AS transactionid,
  FROM
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga,
    UNNEST(ga.hit)),
  --This temp table filters out cordinates with null values
  clean_data_filtering_null_location AS (
  SELECT
    *
  FROM
    DENORM_TABLE
  WHERE
    (locationLat IS NOT NULL
      OR locationLon IS NOT NULL)),
  --- Creating the user journey by ranking with the time
  user_journey AS (
  SELECT
    * EXCEPT(screen),
    CASE
      WHEN screen IN ('home', 'shop_list') THEN 'Home'
      WHEN screen IN ('checkout') THEN 'Checkout'
      WHEN screen IN ('order_confirmation') THEN 'Order Confirmation'
  END
    AS screen,
    RANK() OVER (PARTITION BY fullvisitorid, visitId, visitStartTime ORDER BY time ASC) AS step
  FROM
    clean_data_filtering_null_location
  WHERE
    screen IN ('order_confirmation',
      'home',
      'shop_list',
      'checkout')),
  final_result AS (
  SELECT
    a.fullvisitorid,
    a.visitId,
    a.screen,
    a.visitStartTime,
    --- Check to see if Cordinates has ever changed
    CASE
      WHEN CONCAT(a.locationLat, a.locationlon) != CONCAT(b.locationLat, b.locationlon) THEN 'Changed'
    ELSE
    'Unchanged'
  END
    AS change_flag,
    a.transactionid,
    a.locationLat,
    a.locationlon
  FROM
    user_journey a
  LEFT JOIN
    user_journey b
  ON
    CONCAT(b.fullvisitorid, CAST(b.visitStartTime AS string)) = CONCAT(a.fullvisitorid, CAST(b.visitStartTime AS string))
    AND a.step + 1 = b.step),
  unique_user_txid AS (
    # This Table Compute the final results for those that changed address and displays details of their orders
  SELECT
    fullvisitorid,
    transactionid,
    locationLat,
    locationlon
  FROM
    final_result
  WHERE
    transactionid IS NOT NULL
  GROUP BY
    fullvisitorid,
    transactionid,
    locationLat,
    locationlon),
  users_that_changed_address AS (
  SELECT
    *
  FROM
    unique_user_txid
  WHERE
    fullvisitorid IN (
    SELECT
      fullvisitorid
    FROM
      final_result
    WHERE
      change_flag = 'Changed') )
SELECT
  fullvisitorid,
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
  AS order_delivered,
  ST_EQUALS(ST_GEOGPOINT(locationLon,
      locationLat),
    tr.geopointDropoff) AS location_matched,
FROM
  users_that_changed_address ga
LEFT JOIN
  `dhh-analytics-hiringspace.BackendDataSample.transactionalData` tr
ON
  ga.transactionid = tr.frontendOrderId
