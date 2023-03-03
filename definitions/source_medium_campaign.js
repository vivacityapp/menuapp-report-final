  
  const sources = ["analytics_340662362", "analytics_321800234"];
   // test //

publish(dataform.projectConfig.vars.ga4Table, {
    type: "table",
    
  })

  .query(ctx => {
      return `

      SELECT
        user_pseudo_id as _id,
        count(event_name) as pageviews,
        (select 1) AS uniquePageviews,
        traffic_source.medium as medium,
        traffic_source.source as source,
        device.language as language,
        "analytics_340662362" as qwe,
        DATETIME_TRUNC(DATETIME(timestamp_micros(event_timestamp),"Europe/London"),minute) as createdAt,
        page_location AS pagePath,
        struct(
          split(page_location, '/')[safe_offset(1)] AS rift1,
          split(page_location, '/')[safe_offset(2)] AS rift2,
          split(page_location, '/')[safe_offset(3)] AS rift3,
          split(page_location, '/')[safe_offset(4)] AS rift4,
          split(page_location, '/')[safe_offset(5)] AS rift5,
          split(page_location, '/')[safe_offset(6)] AS rift6,
          split(page_location, '/')[safe_offset(7)] AS rift7,
          split(page_location, '/')[safe_offset(8)] AS rift8
        ) AS rift_pagePath
      FROM 
      (SELECT * ,
      (select REGEXP_REPLACE(e.value.string_value,r'https://m.vivacityapp.com|https://inhouse.vivacityapp.com|^.*?dindins.club.*?$'  , '') from unnest(event_params) as e where key = 'page_location') AS page_location
      FROM dindins-club.analytics_${(dataform.projectConfig.vars.ga4Id)}.events_20230227)
      WHERE event_name = 'page_view'
      group by
        user_pseudo_id,
        event_name,
        medium,
        source,
        language,
        createdAt,
        pagePath

  
      `
        })
        
  
  
  
  

