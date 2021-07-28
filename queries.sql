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
      INDEX = 19) AS locationLat
  FROM
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga,
    UNNEST(ga.hit) ),
  -- This Table Creates the Journey and Ranks them in sequencial order e.g Home, Checkout, Order Placement
  journey AS (
  SELECT
    *,
    DENSE_RANK() OVER(PARTITION BY fullvisitorid, visitId, visitStartTime ORDER BY CASE WHEN screen IN ('home', 'shop_list') THEN '1st Stage'
        WHEN screen IN ('checkout') THEN '2nd Stage'
        WHEN screen IN ('order_confirmation') THEN '3rd Stage'
    END
      ) AS rank
  FROM
    denorm_table
  WHERE
    screen IN ('order_confirmation',
      'home',
      'shop_list',
      'checkout')),
  --- This table filters out visitors that completed the Journey i.e got to the Order Placement Screen.
  final AS (
  SELECT
    *,
    CONCAT(coalesce(locationLat),',',coalesce(locationLon)) AS curr_cord,
    --This Function extracts the previous cell value in a given row, this would be useful when checking for Change in address
    LAG(CONCAT(coalesce(locationLat),',',coalesce(locationLon))) OVER(PARTITION BY fullvisitorid, visitId, visitStartTime, rank ORDER BY time ASC) AS prev_cord
  FROM
    journey
    --Filter for only customers that reached the last funnel
  WHERE
    CONCAT(coalesce(fullvisitorid),' ',coalesce(visitStartTime)) IN (
    SELECT
      CONCAT(coalesce(fullvisitorid),' ',coalesce(visitStartTime))
    FROM
      journey
    WHERE
      rank = 3) )
SELECT
  fullvisitorid,
  visitStartTime,
  ST_GeogPoint(SAFE_CAST(locationlon AS float64),
    SAFE_CAST(locationLat AS float64)) AS geo_point,
  eventAction,
  CASE
    WHEN screen IN ('home', 'shop_list') THEN 'Home'
    WHEN screen IN ('checkout') THEN 'Checkout'
    WHEN screen IN ('order_confirmation') THEN 'Order Placement'
END
  AS screen,
  curr_cord,
  prev_cord,
  CASE
    WHEN eventAction IN ('address.submitted', 'address_update.submitted') AND curr_cord != prev_cord THEN 'Changed'
  ELSE
  'Not Changed'
END
  address_change_flag
FROM
  final
ORDER BY
  fullvisitorid,
  visitStartTime,
  rank ASC