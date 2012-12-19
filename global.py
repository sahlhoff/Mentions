import json
import pymongo
import re
import unicodedata
import urlparse
from urlobject import URLObject
import sys
import simplejson as json
import urllib2, urllib
import time
import shelve






true = True
false = False
connection = pymongo.Connection("mongodb://localhost", safe=True)

db=connection.ADN
hyper_links = db.hyper_links

file_name = 'ADN.json'
access_token = 'AQAAAAAAAaCWXlSH4haorVa5nuyaCL1d-LDQpN0Lq2SaB_HBz7zxui4s2sQRAazJmKjYgmZi3gPrX2WjSVZM6dyKNUQDvIg81g'
global_stream_url = 'https://alpha-api.app.net/stream/0/posts/stream/global'
 
my_key = db.location.find_one({'type':'my_key'})

min_id =my_key['last']
print min_id
max_id = min_id +200
sleep_time = 5.0
post_count = 0

db.location.update({'type':'my_key'}, {'last':max_id})






while True:
    url = global_stream_url
    query_string = { 'since_id':min_id, 'before_id':max_id, 'count':200 }
    url = url + '?' + urllib.urlencode(query_string)
    print url
    headers = { 'Authorization': 'Bearer '+ access_token }
    req = urllib2.Request(url, None, headers)
    gotresults = False
    while not gotresults:
        try:
            response = urllib2.urlopen(req)
            page = response.read()
            results = json.loads(page)
            results.reverse()
            gotresults = True
            
        
        except urllib2.HTTPError, e:
            logger.error("Got HTTPError: %s. Sleeping %s seconds" % (e, sleep_time))
            time.sleep(sleep_time)
            pass


    for item in results:
        file_count = min_id/1000
        #ADN_Data_File = open("/Users/chadsahlhoff/python/ADN/appnet-logger/bin/ADN/" + str(file_count) + ".json", 'a+')
        #ADN_Data_File.write(json.dumps(item) + '\n')
        #ADN_Data_File.close()
        print item['id']
        print file_count
        post_count += 1
           
    
    min_id = min_id + 200
    max_id = max_id + 200


last_location = item['id']

db.location.update({'last':last_location, 'type':my_key})
  










