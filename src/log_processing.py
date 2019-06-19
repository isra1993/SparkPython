'''
Created on Jun 19, 2019

@author: neumann
'''
import pyspark
import datetime
import re
from pyspark import Row
import matplotlib.pyplot as plt

LOG_FILE_PATH = '/home/neumann/Downloads/data/NASA_access_log_Aug95.gz'
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

MONTH_MAP = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
             'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

spark_context = pyspark.SparkContext()


def parse_apache_time(s):
    return datetime.datetime(int(s[7:11]),
                             MONTH_MAP[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)


if __name__ == '__main__':    
    log_RDD = spark_context.textFile(LOG_FILE_PATH)
    processed_log_RDD = log_RDD.map(parse_apache_log_line).cache()
    
    valid_logs_RDD = processed_log_RDD.filter(lambda r: r[1] == 1).map(lambda r: r[0])
    invalid_logs_RDD = processed_log_RDD.filter(lambda r: r[1] == 0).map(lambda r: r[0])
    
    invalid_logs_number = invalid_logs_RDD.count()
    valid_logs_number = valid_logs_RDD.count()
    # if invalid_logs_number > 0:
        # print 'Invalid records: ', invalid_logs_number
        # print '#' * 60
        # print 'Sample records:'
        # for row in invalid_logs_RDD.take(10):
        #     print row
        
    print 'Read %d records\n\tOK: %d\n\tKO: %d' % (processed_log_RDD.count(), 
                                                   valid_logs_number, 
                                                   invalid_logs_number)
    
    # Count response code numbers 
    count_valid_logs_by_key_RDD = valid_logs_RDD.map(lambda r: (r.response_code, 1)) \
                                  .reduceByKey(lambda a, b: a + b)
    
    print '#' * 60
    for (code, count) in count_valid_logs_by_key_RDD.collect():
        print '%d -> %d' % (code, count)

    # Generate results graph
    response_codes = count_valid_logs_by_key_RDD.map(lambda (x, y): x).collect()
    response_code_percentages = count_valid_logs_by_key_RDD.map(lambda (x, y): (float(y)/valid_logs_number)).collect() 
    
    plt.figure(figsize=(4.5, 4.5), facecolor='white', edgecolor='white')
    
    plot_colors = ['yellowgreen', 'lightskyblue', 'gold', 'purple', 'lightcoral', 
                   'yellow', 'black']
    plt.pie(x=response_code_percentages, labels=response_codes, shadow=False, 
            startangle=125, colors=plot_colors, explode=(0.05, 0.05, 0.1, 0, 0, 0, 0))
    plt.legend(response_codes, loc=(0.80, -0.1), shadow=True)
    plt.savefig('response_codes.jpg')
    plt.show()
