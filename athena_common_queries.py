"""
Common queryies + utilities for querying Athena

Tends to return as pandas dataframe


Note that the queries are designed for athena, where there's no indexing and the main / sole
optimisation is to hit the date partition.  Other databases you'd want to filter more at the first
level query.


#TODO: deal with timezones to get days in say SG time while the source is in UTC
"""

import pandas as pd 

from athena_querying import * # needed for table definitions etc

from data_parsing import get_metadata_from_url

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

def create_generic_event_query(from_datetime, to_datetime, event_names = None, include_device_type_data=False, include_user_agent = False, interpret_urls=True, include_ip_address = False):
    """
    Leave country codes, and event_names as null if you don't want to filter
    
    event_names must be exact matches
    
    Device type data includes the user agent as well.
    
    It might be better to use python to parse the data though, and get_metadata_from_url() might be a better tool than the SQL code here
    
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
    , substr(sent_at, 1, 10) as date
    , "type"
    , "body"."event_name"
    , "body"."data"."status"
    , "user"."anonymous_id"
    , "user"."amp_id"
    , "context"."page_url"
    , "context"."referrer"
 
    """
    
    if interpret_urls:
        query += """
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
    if include_user_agent or include_device_type_data:
        query += """
        , context.user_agent as user_agent
        """
    
    if include_device_type_data:
        query += """
        , context.device.type as device_type
        , context.browser.name as browser_name
        , context.browser.major as browser_major_version
        , context.browser.version as browser_version
        , context.operating_system.name as operating_system_name
        , context.operating_system.version as operating_system_version
       
        """
        
    if include_ip_address:
        query += """
        , context.ip_address
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
        query += "\n AND country_code in ({})".format("'"+ "', '".join(country_codes)+"'")  #probably inefficient putting in having rather than main query.
    

    
    if limit:
        query += "\n LIMIT %i"%limit
    
    return query








#---------------------------------------------------------------------------




    
def get_blog_events(country_code, from_datetime, to_datetime, athena_query, limit=None, only_pageviews = False):
    #TODO: ideally you break into chunks during reading and replace various strings with post_id
    start_time =  datetime.now()
    print("Starting athena events query at %s"% start_time.isoformat())
    print("This might take some time")
    
    query = create_blog_pageview_and_scroll_query(country_code, from_datetime, to_datetime, limit=limit, only_pageviews=only_pageviews)
    
    print("Query : "+ query)
    events = athena_query.query(query, print_debug_messages=True)
    #events = pd.read_sql(query, athena_conn)
    
    end_time =  datetime.now()
    print("Ended query at %s"%end_time.isoformat())
    dt = end_time-start_time
    #print("Query took %s" %dt)
    
    
    #get_metadata_from_url  #[page_type, slug, slug_root, ab_test, country_code]
    """
    print("Joining in metadata, which might be slow")  #if it is really slow, then can move it into 
    metadata_start_time = datetime.now()
    events = events.apply(lambda x: pd.Series(get_metadata_from_url(x.page_url)), axis=1)
    events.rename(columns={0:"page_type", 1:"slug", 2:"slug_root", 3:"ab_test", 4:"country_code"}, inplace=True)
    
    metadata_end_time = datetime.now()
    metadata_time_taken = (metadata_end_time-metadata_start_time).total_seconds()
    print("Ended adding metadata at %s"%end_time.isoformat())
    print("Taking %i seconds"%metadata_time_taken)
    """
    

    print("")
    print("Setting data types")
    datatype_start_time = datetime.now()
    
    # TODO: move the data type stuff into the CSV reading part to reduce memory at that stage (built in function supports it)
    data_type_map = { # Don't have to set them all 
       # "sent_at_timestamp":"datetime64[ns, <tz>]",
        #"sent_at":"string", #keep as string
        "date":"category",
        "type" : "category",
        "event_name" :"category",
        "status":"category",
        #anonymous_id
        #amp_id
        #page_url # could make category ymmv.
        "referrer":"category",
        "is_amp":"bool",
        "is_test":"bool",
        "is_control":"bool",
        "slug":"category",
        "country_code":"category",
        "is_blog":"bool",
        "is_unbounce":"bool",
        "is_embed":"bool",
        "user_agent":"category", #There's likely a lot of options, but over a large number of rows a lot of duplicates
        "device_type":"category",
        "browser_name":"category",
        "browser_major_version":"category",
        "browser_version":"category",
        "operating_system_name":"category",
        "operating_system_version": "category",
        "article_read_depth":"int16",
        
    }
    
    events = events.astype(data_type_map, copy=False)
    
    datatype_end_time = datetime.now()
    datatype_time_taken = (datatype_end_time-datatype_start_time).total_seconds()
    print("Ended query at %s"%datatype_end_time.isoformat())
    print("Taking %i seconds"%datatype_time_taken)
    

    end_time = datetime.now()
    total_time_seconds = (end_time -start_time).total_seconds()
    print("In total it took %i seconds"%total_time_seconds)
    return events
    