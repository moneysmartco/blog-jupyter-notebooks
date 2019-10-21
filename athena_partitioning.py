"""
S3 and Athena connection functions (may not be of use)
"""


def get_list_of_expected_s3_folders_and_date_keys():
    # s3://ms-data-pipeline-production/ms-data-stream-production-processed/2019/08/02
    root_path = event_log_s3_path
    paths = []
    start_date = datetime(year=2019, month = 6, day = 18)
    now = datetime.now() #TODO: the timezone seems wrong :(
    print(now)
    day_increment = timedelta(days=1)
    processing_day = start_date
    while processing_day<now: #should include the current day
        day_suffix = "%.4i/%.2i/%.2i/" % (processing_day.year, processing_day.month, processing_day.day)
        day_index = "%.4i-%.2i-%.2i" % (processing_day.year, processing_day.month, processing_day.day)
        paths.append([root_path+"/"+day_suffix, day_index])
        processing_day += day_increment
    return paths
expected_paths_sent_ats = get_list_of_expected_s3_folders_and_date_keys()


expected_paths_sent_ats[:5]+ expected_paths_sent_ats[-5:]