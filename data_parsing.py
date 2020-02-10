from urllib.parse import urlparse, parse_qs

def get_metadata_from_url(url):
    p = urlparse(url.lower())
    
    #urlparse("https://www-new.moneysmart.sg/rabbit/mouse/?a=b")
    #ParseResult(scheme='https', netloc='www-new.moneysmart.sg', path='/rabbit/mouse/', params='', query='a=b', fragment='')
    
    
    nl = p.netloc
    
    page_type = ""
    stripped_path = p.path.strip("/")
    
    slug = "/"+stripped_path
    
    if slug.startswith("/en/") or slug.startswith("/zh-hk/"):
        slug_root = "/"+stripped_path.split("/")[1]
    elif slug=="/en" or slug=="/zh-hk":
        slug_root = "/"
    else:
        slug_root = "/"+stripped_path.split("/")[0]
    
    
    
    #blog (for SG and HK)
    if "moneysmart.tw" in nl or "moneysmart.ph" in nl or "moneysmart.id" in nl or 'blog.moneysmart' in nl or 'blog-new' in nl or 'blog3' in nl:
        page_type = "blog"
    
    #LPS
    elif stripped_path.endswith("ms"):
        page_type = "lps"
    
    #interstitial
    elif "iss.moneysmart" in nl or stripped_path.endswith("apply") or stripped_path.endswith("redirect"):
        page_type = "iss"
        
    #unbounce
    elif "get.moneysmart" in nl:
        page_type = "unbounce"
     
    
    #embed     , "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 5) like '/embed/%' as is_embed
    elif slug_root.startswith("/embed/"): #As it uses slug_root, I think will be safe with HK locales.
        page_type = "embed"
    
    #else shop
    else:
        page_type = "shop"
        
        
        
    
    #ab test side , CAST("strpos"("context"."page_url", '://www-new.') AS boolean) OR CAST("strpos"("context"."page_url", '://www3.') AS boolean)  OR CAST("strpos"("context"."page_url", '://blog3.') AS boolean) as "is_test"
    if "www-new." in nl or "www3." in nl or "blog3." in nl:
        ab_test = "test"
    else:
        ab_test = "control"
    

    
    
    
    
    """
     , CASE WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.sg%' THEN 'sg' 
        WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.hk%' THEN 'hk' 
        WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.id%' THEN 'id' 
        WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.ph%' THEN 'ph'
        WHEN "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 4) LIKE '%moneysmart.tw%' THEN 'tw' 
        ELSE null END as country_code
    """
    
    country_code = ""
    if "moneysmart.sg" in nl:
        country_code = "sg"
    elif "moneysmart.hk" in nl:
        country_code = "hk"
    elif "moneysmart.id" in nl:
        country_code = "id"
    elif "moneysmart.ph" in nl:
        country_code = "ph"
    elif "moneysmart.tw" in nl:
        country_code = "tw"
    elif "moneysmart.com" in nl:
        country_code = "ww" #worldwide
    else:
        country_code = "??"
    
    
    #return {"page_type":page_type, "path":path, "ab_test":ab_test, "country_code":country_code}
    return [page_type, slug, slug_root, ab_test, country_code]
    
    
