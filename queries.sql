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