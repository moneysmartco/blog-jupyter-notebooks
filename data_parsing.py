from urllib.parse import urlparse, parse_qs

def get_metadata_from_url(url, return_canonical_url = False, return_dim_pages_url = False):
    p = urlparse(url.lower())
    
    #urlparse("https://www-new.moneysmart.sg/rabbit/mouse/?a=b")
    #ParseResult(scheme='https', netloc='www-new.moneysmart.sg', path='/rabbit/mouse/', params='', query='a=b', fragment='')
    
    
    nl = p.netloc
    
    page_type = ""
    stripped_path = p.path.strip("/")
    
    # --- slug ----
    
    slug = "/"+stripped_path
    
    if slug.startswith("/en/") or slug.startswith("/zh-hk/"):
        slug_root = "/"+stripped_path.split("/")[1]
    elif slug=="/en" or slug=="/zh-hk":
        slug_root = "/"
    else:
        slug_root = "/"+stripped_path.split("/")[0]
    
    #TODO: I feel like should remove the language from the slug, and send it separately
    
    
    # --- parse the page_type -------
    #blog (for SG and HK)
    if "moneysmart.tw" in nl or "moneysmart.ph" in nl or "moneysmart.id" in nl or 'blog.moneysmart' in nl or 'blog-new' in nl or 'blog3' in nl:
        if slug == "/":
            page_type = "blog_home_page"
        elif slug_root in (["/category"]):
            page_type = "blog_category_page"
        elif slug_root in (["/tag"]):
            page_type = "blog_tag_page"
        elif "moneysmart.tw" in nl and slug_root not in ["/tag", "/articles"]: # e.g. https://www.moneysmart.tw/earn-money/%e7%90%86%e8%b2%a1%e8%a7%80%e5%bf%b5/ is a category (tag pages are fine)
            page_type = "blog_category_page"
        elif ("moneysmart.sg" in nl or "moneysmart.hk" in nl) and slug in [slug_root, "/en" + slug_root, "/zh-hk"+slug_root]: # e.g. blog.moneysmart.hk/en/mastercard is tag and category
            page_type = "blog_category_tag_page"
        else:
            page_type = "blog_article"  #TODO: ideally would do article vs category vs home page
    
    elif slug == "/" and "learn." not in nl:
        page_type = "home_page"
    
    #LPS
    elif stripped_path.endswith("-ms"):
        page_type = "lps"
    
    #learn
    elif "learn." in nl:
        page_type= "learn" #there's categories within it, but really it should be dead, so being lazy
        
    
    
    #interstitial
    elif "iss.moneysmart" in nl or stripped_path.endswith("apply") or stripped_path.endswith("redirect"):
        page_type = "iss"
        
    #unbounce
    elif "get.moneysmart" in nl:
        page_type = "unbounce"
        
    #mortgage / refinance / home loans specials
    # not sure if they are used elsewhere, but I didn't find any evidence
    elif ("www.moneysmart" in nl) and (any([z in slug for z in ["refinance", "home-loan", "mortgage"]])) and (any([slug.endswith(z) for z in ["-calculator", "-trend"]])):
        if slug.endswith("-calculator"):
            page_type="calculator"
        elif slug.endswith("-trend"):
            page_type="trend"
                                                                                                 

    
    #embed     , "regexp_extract"("context"."page_url", '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?', 5) like '/embed/%' as is_embed
    elif slug_root=="/embed": #As it uses slug_root, I think will be safe with HK locales.
        page_type = "embed"
                                                                                     
                                                                                                       
    #else shop
    else:
        # listing and PDP logic requires knowing what products you're expecting, so skipping that (and it's in the data warehouse)
        page_type = "shop"
        
        
    
    #ab test side , CAST("strpos"("context"."page_url", '://www-new.') AS boolean) OR CAST("strpos"("context"."page_url", '://www3.') AS boolean)  OR CAST("strpos"("context"."page_url", '://blog3.') AS boolean) as "is_test"
    if "www-new." in nl or "www3." in nl or "blog3." in nl:
        ab_test = "test"
    else:
        ab_test = "control"
    

    
    #--- Country code ---
    
    # NB needs to be robust to urls from other sites coming in (staging, totally off our site etc)
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
    
    
    ret = [page_type, slug, slug_root, ab_test, country_code]
    return ret
    
def get_canonical_url(url):
    p = urlparse(url.lower())
    nl = p.netloc
    stripped_path = p.path.strip("/")
    slug = "/"+stripped_path
    
    
    # I'm trying to match what the data warehouse does here.  YMMV.  And AToW it's not really canonicalising.
    canonical_nl = nl
    replacements = [
        ["www3","www"],
         ["www-new","www"],
         ["blog3","blog"],

    ]
    for from_str, to_str in replacements:
        canonical_nl = canonical_nl.replace(from_str, to_str)
    canonical_url = canonical_nl + slug
    canonical_url = canonical_url.rstrip("/") # for homepages (debatable)
    #ParseResult(scheme='https', netloc='www-new.moneysmart.sg', path='/rabbit/mouse/', params='', query='a=b', fragment='')

    return canonical_url

def get_dim_page_url(url):
    p = urlparse(url.lower())
    nl = p.netloc
    stripped_path = p.path.strip("/")
    slug = "/"+stripped_path
    
    dim_pages_url = nl+ slug
    dim_pages_url = dim_pages_url.strip("/") #not totally clear, but seems to be what should do for homepage
    return dim_pages_url



def run_tests():
                                           
    # TODO: should really do proper unit test... being lazy                                                                                                 
    test_urls = [
            ["blog_article" , "https://www.moneysmart.tw/articles/%e9%99%8d%e6%81%af-%e5%88%a9%e7%8e%87-%e8%81%af%e6%ba%96%e6%9c%83-%e5%a4%ae%e8%a1%8c/"],
            ["blog_article", "https://blog3.moneysmart.sg/fixed-deposits/best-fixed-deposit-accounts-singapore/"],
            ["blog_article","https://blog.moneysmart.sg/savings-accounts/dbs-multiplier-ocbc360-uob-one-covid-19"],
            ["blog_tag_page", "https://www.moneysmart.tw/tag/%e5%88%a9%e7%8e%87/"],
            ["blog_home_page", "https://www.moneysmart.tw/"],
            ["blog_home_page", "https://blog3.moneysmart.sg/"],
            ["blog_home_page", "https://blog.moneysmart.sg/"],
            ["blog_category_page", "https://blog3.moneysmart.sg/category/opinion/"],
            ["shop","https://www.moneysmart.sg/credit-cards"], #ideally listing, but can't infer easily
            ["unbounce", "https://get.moneysmart.sg/foo/bar"],
            ["lps","https://www.moneysmart.hk/en/foo/bar-ms"],
            ["iss", "https://iss.moneysmart.sg/credit-cards/citi-cashback-plus-card/redirect?provider_slugs[]=-1&category_slug=best-credit-cards"],
            ["shop", "https://www.moneysmart.sg/credit-cards/air-miles?provider_slugs%5B%5D=-1&category_slug=air-miles&sort_by=recommended"],
            ["shop", "https://www-new.moneysmart.sg/credit-cards/air-miles"],
            ["learn", "https://learn.moneysmart.sg/cars/how-to-save-money-on-buying-a-car/"],
            ["trend","https://www.moneysmart.sg/home-loan/sibor-trend"],
            ["calculator", "https://www.moneysmart.sg/home-loan/mortgage-calculator"],
            ["blog_tag_page","https://blog3.moneysmart.sg/tag/transportation/"],
            ["embed", "https://www.moneysmart.sg/embed/20bcfa659b31c5aecc944baf90c9c8bf"],
            ["blog_category_tag_page", "https://blog.moneysmart.hk/en/air-miles"],
            ["blog_category_tag_page", "https://blog.moneysmart.hk/zh-hk/dining"],
            ["blog_category_tag_page", "https://blog.moneysmart.sg/business"],
    ]
    
    for expected_type, url in test_urls:
        res = get_metadata_from_url(url, return_canonical_url=True, return_dim_pages_url = True)
        [page_type, slug, slug_root, ab_test, country_code] = res
        canonical_url = get_canonical_url(url)
        dim_page_url = get_dim_page_url(url)
        print(url)
        print("\tPage type inferred = %s, expected = %s" % (page_type, expected_type))
        if expected_type != page_type:
            raise Exception("Expected")
        for r in res:
            print("\t"+r)
        print("\tcanonical: "+canonical_url)
        print("\tdim_page:  "+ dim_page_url)
        
                                                                                                       