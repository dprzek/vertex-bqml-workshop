
run_bq_query(
    f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}` as
    SELECT
      sessions.fullVisitorId,
      sessions.ga_session_id,
      sessions.churned,
      sessions.event_date,
      sessions.isMobile,
      sessions.operatingSystem,
      sessions.browser,
      sessions.country,
      sessions.city,
      sessions.firstSource,
      sessions.firstMedium,
      sessions.sessionNumber,
      sessions.latest_ecommerce_progress,
      sessions.isFirstVisit,
      sessions.productPagesViewed,
      sessions.addedToCart,
      sessions.purchase,

      users.totalHits,
      users.totalPageviews,

      visits.totalVisits,

      engagement.totalTimeOnSite, # bucket
      engagement.engagement_seconds,
      engagement.engagement_minutes,
      engagement.engagement_avg_seconds,
      engagement.engagement_avg_minutes,

      source.source,
      source.medium,
      source.campaign,

    FROM (
          SELECT
            fullVisitorId,
            session_id,
            ga_session_id,
            churned,
            event_date,
            isMobile,
            operatingSystem,
            browser,
            country,
            city,
            firstSource,
            firstMedium,
            MAX(ga_session_number) sessionNumber,
            MAX(latest_ecommerce_progress) latest_ecommerce_progress,
            MAX(isFirstVisit) isFirstVisit,
            MAX(productPagesViewed) productPagesViewed,
            MAX(addedToCart) addedToCart,
            MAX(purchase) purchase
          FROM (
                SELECT
                  user_pseudo_id fullVisitorId,
                  CONCAT(user_pseudo_id, "-", (SELECT value.int_value FROM unnest(event_params) WHERE key="ga_session_id")) session_id,
                  (select value.int_value from unnest(event_params) where key = 'ga_session_id') ga_session_id,
                  (select value.int_value from unnest(event_params) where key = 'ga_session_number') ga_session_number,
                  IF (TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP_ADD(TIMESTAMP_MICROS(user_first_touch_timestamp), INTERVAL 24 HOUR), 1, 0 ) churned,
                  event_date,
                  CASE device.category WHEN 'mobile' THEN 0  ELSE 1 END AS isMobile,
                  device.operating_system operatingSystem,
                  device.web_info.browser browser,
                  geo.country AS country,
                  IFNULL(geo.city, '') city,
                  traffic_source.source firstSource,
                  traffic_source.medium firstMedium,
                  CASE event_name WHEN 'first_visit' THEN 1  ELSE 0 END isFirstVisit,
                  CASE event_name WHEN 'view_item' THEN 1 ELSE 0 END productPagesViewed,
                  CASE event_name WHEN 'add_to_cart' THEN 1  ELSE 0 END addedToCart,
                  CASE event_name WHEN 'purchase' THEN 1  ELSE 0 END purchase,
                  CASE
                    WHEN event_name = 'view_item' THEN 1
                    WHEN event_name = 'add_to_cart' THEN 2
                    WHEN event_name = 'view_cart' THEN 3
                    WHEN event_name = 'begin_checkout' THEN 4
                    WHEN event_name = 'purchase' THEN 5
                  ELSE 0 END AS latest_ecommerce_progress

              FROM
                `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
              WHERE
                platform = "WEB"
                and (select value.int_value from unnest(event_params) where key = 'ga_session_id') IS NOT NULL
                -- and user_pseudo_id = '335679038.1695746809'
              GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)

              GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
            ) sessions

    LEFT JOIN (

        SELECT
        user_pseudo_id fullVisitorId,
        CONCAT(user_pseudo_id, "-", (SELECT value.int_value FROM unnest(event_params) WHERE key="ga_session_id")) session_id,
        COUNT(user_pseudo_id) totalHits,
        COUNTIF(event_name = 'page_view') totalPageviews
        FROM
          `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE
          platform = "WEB"
          -- and user_pseudo_id = '335679038.1695746809'
        GROUP BY 1,2
    ) users USING(session_id)

    LEFT JOIN (

        SELECT
        user_pseudo_id AS fullVisitorId,
        CONCAT(user_pseudo_id, "-", (SELECT value.int_value FROM unnest(event_params) WHERE key="ga_session_id")) session_id,
        COUNT(DISTINCT (select value.int_value from unnest(event_params) where key = 'ga_session_id')) totalVisits
        FROM
          `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE
        platform = "WEB"
        -- and user_pseudo_id = '335679038.1695746809'
        GROUP BY 1,2
    ) visits USING(session_id)

    LEFT JOIN (

        SELECT
        user_pseudo_id fullVisitorId,
        session_id,
        MAX(engagement_time_msec) totalTimeOnSite,
        cast(coalesce(sum(engagement_time_seconds),0) as INT64) engagement_seconds,
        cast(coalesce(sum(engagement_time_minutes),0) as INT64) engagement_minutes,
        cast(coalesce(avg(engagement_time_seconds),0) as INT64) engagement_avg_seconds,
        cast(coalesce(avg(engagement_time_minutes),0) as INT64) engagement_avg_minutes
        FROM (
              SELECT
              user_pseudo_id,
              CONCAT(user_pseudo_id, "-", (SELECT value.int_value FROM unnest(event_params) WHERE key="ga_session_id")) session_id,
              (select value.int_value from unnest(event_params) where key = 'engagement_time_msec') engagement_time_msec,
              max((select value.string_value from unnest(event_params) where key = 'session_engaged')) as session_engaged,
              sum((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'))/1000 as engagement_time_seconds,
              sum((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'))/(1000 * 60) as engagement_time_minutes
              FROM
              `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
              WHERE
              platform = "WEB"
              -- and user_pseudo_id = '335679038.1695746809'
              group by 1,2,3
        )
        GROUP BY 1,2
    ) engagement USING(session_id)

    LEFT JOIN (

        SELECT
        user_pseudo_id AS fullVisitorId,
        CONCAT(user_pseudo_id, "-", (SELECT value.int_value FROM unnest(event_params) WHERE key="ga_session_id")) session_id,
        (array_agg((select value.string_value from unnest(event_params) where key = 'source') ignore nulls order by event_timestamp)[offset(0)]) as source,
        (array_agg((select value.string_value from unnest(event_params) where key = 'medium') ignore nulls order by event_timestamp)[offset(0)]) as medium ,
        (array_agg((select value.string_value from unnest(event_params) where key = 'campaign') ignore nulls order by event_timestamp)[offset(0)]) as campaign
        FROM
          `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE
        platform = "WEB"
        -- and user_pseudo_id = '335679038.1695746809'
        GROUP BY 1,2
    ) source USING(session_id)
    """, show=True
)
