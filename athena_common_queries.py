"""
Common queryies + utilities for querying Athena

Tends to return as pandas dataframe


Note that the queries are designed for athena, where there's no indexing and the main / sole
optimisation is to hit the date partition.  Other databases you'd want to filter more at the first
level query.
"""

import pandas as pd 

from athena_querying import *

def create_partition_filter(from_datetime, to_datetime):
    
    start_year_inclusive = from_datetime.year
    end_year_inclusive = to_datetime.year
    start_month_inclusive = from_datetime.month
    end_month_inclusive = to_datetime.month
    start_day_inclusive = from_datetime.day
    end_day_inclusive = to_datetime.day
    
    partition_constraints=""
    
        # - from block - 
    partition_constraints = "\n  ("
    partition_constraints += "\n " + athena_year_partition + " >= '%.4i'"%start_year_inclusive
    partition_constraints += "\n AND " + athena_month_partition + " >= '%.2i'"%start_month_inclusive
    partition_constraints += "\n AND " + athena_day_partition+ " >= '%.2i'"%start_day_inclusive
    
    # or based on month
    partition_constraints += "\n OR ("
    partition_constraints += "\n " + athena_year_partition + " >= '%.4i'"%start_year_inclusive
    partition_constraints += "\n AND " + athena_month_partition + " > '%.2i'"%start_month_inclusive
    partition_constraints += "\n ) "
    
    # or based on year
    partition_constraints += "\n OR ("
    partition_constraints += "\n " + athena_year_partition + " > '%.4i'"%start_year_inclusive
    partition_constraints += "\n ) "
    partition_constraints += "\n)" 
    # - end from block - 
    
    
    # - to block - 
    #TOD: >> I think this needs some adjustment for when you roll between months.
    partition_constraints += "\n AND ((" + athena_year_partition + " <= '%.4i'"%end_year_inclusive
    partition_constraints += "\n\t AND " + athena_month_partition + " <= '%.2i'"%end_month_inclusive
    partition_constraints += "\n\t AND " + athena_day_partition+ " <= '%.2i'"%end_day_inclusive
    partition_constraints += "\n) "
    
    
    partition_constraints += '\n OR ('
    
    partition_constraints += "\n\t " + athena_year_partition + " <= '%.4i'"%end_year_inclusive
    partition_constraints += "\n\t AND " + athena_month_partition + " < '%.2i'"%end_month_inclusive
    partition_constraints += "\n) "
    
    partition_constraints += '\n OR ('
    partition_constraints += "\n\t " + athena_year_partition + " < '%.4i'"%end_year_inclusive
    partition_constraints += "\n) "
    
    partition_constraints += '\n)'
    
    return partition_constraints

def create_generic_event_query(from_datetime, to_datetime, event_names = None, include_device_type_data=False):
    """
    Leave country codes, and event_names as null if you don't want to filter
    
    event_names must be exact matches
    
    """
    #These are just used for the partition selection and further filter is done on the datetime level.
 
    """

    expected_country_codes = ['sg','hk', 'id', 'tw', 'ph']
    
    if country_codes:
        bad_country_codes = [z for z in country_codes if z not in expected_country_codes]
        if bad_country_codes:
            raise Exception("Didn't recognise country codes "+repr(bad_country_codes)+".")
            
    """
            
    query = """
    
    SELECT 
          CAST("from_iso8601_timestamp"("sent_at") AS timestamp) "sent_at_timestamp"
    , "sent_at"
    , "type"
    , "body"."event_name"
    , "body"."data"."status"
        , "user"."anonymous_id"
    , "user"."amp_id"
        , "context"."page_url"
    , "context"."referrer"
    , CAST("strpos"("context"."page_url", '?amp') AS boolean) "is_amp"
    , CAST("strpos"("context"."page_url", '://www-new.') AS boolean) OR CAST("strpos"("context"."page_url", '://www3.') AS boolean)  OR CAST("strpos"("context"."page_url", '://blog3.') AS boolean) as "is_test"
    , CAST("strpos"("context"."page_url", '://www.') AS boolean) OR CAST("strpos"("context"."page_url", '://blog.') AS boolean) "is_control"
    
    
    
    , regexp_replace(CASE 
        WHEN context.amp_version is not null THEN regexp_extract(context.canonical_url, '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 5)
        ELSE regexp_extract(context.page_url, '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 5)
        END
        , '/$', '') as slug
    
    
    , CASE WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.sg%' THEN 'sg' 
        WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.hk%' THEN 'hk' 
        WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.id%' THEN 'id' 
        WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.ph%' THEN 'ph'
        WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.tw%' THEN 'tw' 
        ELSE null END as country_code
    
    
    
    , "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%blog%' as is_blog
    , "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%get%' as is_unbounce
    , "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 5) like '/embed/%' as is_embed
    
    """
    
    if include_device_type_data:
        query += """
        , context.device.type as device_type
        , context.browser.name as browser_name
        , context.browser.major as browser_major_version
        , context.browser.version as browser_version
        
        , context.operating_system.name as operating_system_name
        , context.operating_system.version as operating_system_version
        , context.user_agent as user_agent
        """
    query+="""
    
    FROM
      """+athena_database+ "." +athena_raw_events_table+ """
    
    
    WHERE true -- makes query composition easier
    """

        
    if event_names:
        query += "\n AND body.event_name in ({})".format("'"+ "', '".join(event_names)+"'")
        
    
    partition_constraints = create_partition_filter(from_datetime, to_datetime)

    
    
    query += "\n AND " + partition_constraints
    
    
    

    
    query += "\n AND CAST(from_iso8601_timestamp(sent_at) AS timestamp)  between CAST(from_iso8601_timestamp('%s') AS timestamp) AND CAST(from_iso8601_timestamp('%s') AS timestamp)" % (from_datetime.isoformat(), to_datetime.isoformat())
    
    """
    query += "\n HAVING true "
    if country_codes:
        query += "\n AND country_code in ({})".format("'"+ "'".join(country_codes)+"'")  #probably inefficient putting in having rather than main query.
    """
    
    # *** filter staging urls and admin urls??
    
    return query






def create_blog_pageview_and_scroll_query(country_codes, from_datetime, to_datetime, limit=None, include_device_type_data = True, only_pageviews = False):
    """ 
    This has been taken from the view in athena to make it more flexible.
    country_code should be e.g. TW / SG / ...
    
    It filters out just events from the blog
    """
    print("Getting the pageview and read events from %s to %s"%(from_datetime.isoformat(), to_datetime.isoformat()))
    
    
    
    
    main_query = create_generic_event_query(from_datetime, to_datetime, event_names=(["PageView",] if only_pageviews else ["PageView", "Reading"]), include_device_type_data =include_device_type_data)
    
    
    
    expected_country_codes = ['sg','hk', 'id', 'tw', 'ph']
    
    if country_codes:
        bad_country_codes = [z for z in country_codes if z not in expected_country_codes]
        if bad_country_codes:
            raise Exception("Didn't recognise country codes "+repr(bad_country_codes)+".")
        
    #These are just used for the partition selection and further filter is done on the datetime level.
    start_year_inclusive = from_datetime.year
    end_year_inclusive = to_datetime.year
    start_month_inclusive = from_datetime.month
    end_month_inclusive = to_datetime.month
    start_day_inclusive = from_datetime.day
    end_day_inclusive = to_datetime.day
        
    query = """
    SELECT
    *
    , (CASE status
        WHEN 'Page Bottom Reached' THEN 100 
        WHEN 'Article Reading Completed' THEN 100 WHEN 'Article Body 100' THEN 100 
        WHEN 'Article Reading 75%' THEN 75 WHEN 'Article Body 75' THEN 75 
        WHEN 'Article Reading 50%' THEN 50 WHEN 'Article Body 50' THEN 50 
        WHEN 'Article Reading 25%' THEN 25 WHEN 'Article Body 25' THEN 25
        WHEN 'Article Reading Started' THEN 0 WHEN 'Article Loaded' THEN 0 
        ELSE 0 END) "article_read_depth"

    

    -- , CAST((("context"."page_url" LIKE '%utm_medium%') OR ("context"."page_url" LIKE '%gclid%')) AS boolean) "has_marketing_param"
    


    
    """

        
    query += """
    FROM
      (
          """+main_query+ """
      )
    WHERE 
        is_blog 
        
        
"""

    if country_codes:
        query += "\n AND country_code in ({})".format("'"+ "'".join(country_codes)+"'")  #probably inefficient putting in having rather than main query.
    

    
    if limit:
        query += "\n LIMIT %i"%limit
    
    return query








#---------------------------------------------------------------------------








def create_blog_pageview_and_scroll_query_old(country_code, from_datetime, to_datetime, limit=None, include_device_type_data = True, only_pageviews = False):
    """ 
    This has been taken from the view in athena to make it more flexible.
    country_code should be e.g. TW / SG / ...
    
    It filters out just events from the blog
    """
    print("Getting the pageview and read events from %s to %s"%(from_datetime.isoformat(), to_datetime.isoformat()))
    
    country_code_to_url_core = {
        "ID":"moneysmart.id", 
        "PH":"moneysmart.ph", 
        "TW":"moneysmart.tw", 
        "SG":"blog.moneysmart.sg", 
        "HK":"blog.moneysmart.hk"
        
    }
    
    
    
    if country_code not in country_code_to_url_core:
        raise Exception("Didn't recognise country code"+repr(country_code)+".")
        
    #These are just used for the partition selection and further filter is done on the datetime level.
    start_year_inclusive = from_datetime.year
    end_year_inclusive = to_datetime.year
    start_month_inclusive = from_datetime.month
    end_month_inclusive = to_datetime.month
    start_day_inclusive = from_datetime.day
    end_day_inclusive = to_datetime.day
        
    query = """
    SELECT
      CAST("from_iso8601_timestamp"("sent_at") AS timestamp) "sent_at_timestamp"
    , "sent_at"
    , "type"
    , "body"."event_name"
    , "body"."data"."status"
    , (CASE "body"."data"."status" 
        WHEN 'Page Bottom Reached' THEN 100 
        WHEN 'Article Reading Completed' THEN 100 WHEN 'Article Body 100' THEN 100 
        WHEN 'Article Reading 75%' THEN 75 WHEN 'Article Body 75' THEN 75 
        WHEN 'Article Reading 50%' THEN 50 WHEN 'Article Body 50' THEN 50 
        WHEN 'Article Reading 25%' THEN 25 WHEN 'Article Body 25' THEN 25
        WHEN 'Article Reading Started' THEN 0 WHEN 'Article Loaded' THEN 0 
        ELSE 0 END) "article_read_depth"
    , "user"."anonymous_id"
    , "user"."amp_id"
    
 
    , "context"."page_url"
    , "context"."referrer"
    , CAST("strpos"("context"."page_url", '?amp') AS boolean) "is_amp"
    , CAST((("context"."page_url" LIKE '%utm_medium%') OR ("context"."page_url" LIKE '%gclid%')) AS boolean) "has_marketing_param"
    
    -- might be able to do based on context.amp_version
    -- This doesn't work for all historical AMP data.
    , CASE 
        WHEN context.amp_version is not null THEN context.canonical_url
        ELSE split_part(context.page_url, '?', 1)
        END as canonical_url
        
    , CASE
        WHEN context.page_url like 'https://blog3.moneysmart%' THEN true
        WHEN context.page_url like 'https://blog.moneysmart%' THEN false
        WHEN context.page_url like 'https://www.moneysmart%' THEN false
        WHEN context.page_url like 'https://www.moneysmart%' THEN false
        ELSE null -- shouldn't happen
        END as is_test

    
    """
    if include_device_type_data:
        query += """
        , context.device.type as device_type
        , context.browser.name as browser_name
        , context.browser.major as browser_major_version
        , context.browser.version as browser_version
        
        , context.operating_system.name as operating_system_name
        , context.operating_system.version as operating_system_version
        , context.user_agent as user_agent
        """
        
    query += """
    FROM
      """+athena_database+ "." +athena_raw_events_table+ """
    WHERE 
        (("body"."event_name" = 'Reading') OR ("body"."event_name" = 'PageView')) 
        
"""
    #TODO: not sure this works with AMP.
    country_constraint = """ AND 
            (context.canonical_url is not null AND
    
            "regexp_extract"(context.page_url, '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%{}%')
            OR
            ("regexp_extract"(context.canonical_url, '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%{}%')
            
            
            """.format(country_code_to_url_core[country_code], country_code_to_url_core[country_code])
    
    
    # - from block - 
    partition_constraints = "\n AND ("
    partition_constraints += "\n " + athena_year_partition + " >= '%.4i'"%start_year_inclusive
    partition_constraints += "\n AND " + athena_month_partition + " >= '%.2i'"%start_month_inclusive
    partition_constraints += "\n AND " + athena_day_partition+ " >= '%.2i'"%start_day_inclusive
    
    # or based on month
    partition_constraints += "\n OR ("
    partition_constraints += "\n " + athena_year_partition + " >= '%.4i'"%start_year_inclusive
    partition_constraints += "\n AND " + athena_month_partition + " > '%.2i'"%start_month_inclusive
    partition_constraints += "\n ) "
    
    # or based on year
    partition_constraints += "\n OR ("
    partition_constraints += "\n " + athena_year_partition + " > '%.4i'"%start_year_inclusive
    partition_constraints += "\n ) "
    partition_constraints += "\n)" 
    # - end from block - 
    
    
    # - to block - 
    #TOD: >> I think this needs some adjustment for when you roll between months.
    partition_constraints += "\n AND " + athena_year_partition + " <= '%.4i'"%end_year_inclusive
    partition_constraints += "\n AND " + athena_month_partition + " <= '%.2i'"%end_month_inclusive
    partition_constraints += "\n AND " + athena_day_partition+ " <= '%.2i'"%end_day_inclusive
    partition_constraints += "\n) "
    
    # - to block - 
    
    
    # TODO: + datetime further constraints
        
        
    
    
    query += " "+country_constraint
    query += " " + partition_constraints
    if only_pageviews:
        query += " AND body.event_name='PageView' "
    
    
    
    #>>> add in something to process amp pages:
        
        """
        https://www-moneysmart-id.cdn.ampproject.org/v/s/www.moneysmart.id/pahami-jenis-jenis-bunga-kredit-bank-dan-cara-perhitungannya-karena-tiap-pinjaman-berbeda-beda/?amp&amp_js_v=0.1&usqp=mq331AQCKAE%3D
        """
    
    if limit:
        query += " LIMIT %i"%limit
    
    return query
    
def get_blog_events(country_code, from_datetime, to_datetime, limit=None, only_pageviews = False):
    #TODO: ideally you break into chunks during reading and replace various strings with post_id and set data types to more efficient ones (category)
    start_time =  datetime.now()
    print("Starting athena events query at %s"% start_time.isoformat())
    print("This might take some time")
    
    query = create_blog_pageview_and_scroll_query(country_code, from_datetime, to_datetime, limit=limit, only_pageviews=only_pageviews)
    
    print("Query : "+ query)
    events = athena.query(query, print_debug_messages=True)
    #events = pd.read_sql(query, athena_conn)
    
    end_time =  datetime.now()
    print("Ended query at %s"%end_time.isoformat())
    dt = end_time-start_time
    #print("Query took %s" %dt)
    return events
    