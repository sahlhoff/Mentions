import json
import pymongo
import re
import unicodedata
import urlparse
from urlobject import URLObject



true = True
false = False



connection = pymongo.Connection("mongodb://localhost", safe=True)

db=connection.ADN
hyper_links = db.hyper_links



def main():


	for i in range(0,1405):


		for item in open('/Users/chadsahlhoff/python/ADN/appnet-logger/bin/ADNLogs/'+ str(i) +'.json','r'):
			line = eval(item)
			entities = line['entities']

			print i

			links_in_entities = entities['links']

			if (len(links_in_entities) != 0):
				for indices in links_in_entities:
					url = indices['url']


					try:
						url = URLObject(url)
						url = unicode(url.with_hostname(url.hostname.lower()))
						
					except:
						url = None

					if url == None:
						break
						
					if url.endswith('.'):
						url=url[:-1]

					if url.endswith(','):
						url=url[:-1]

					if url.endswith('!'):
						url=url[:-1]

					if url.endswith('?'):
						url=url[:-1]

					if url.endswith('/'):
						url=url[:-1]

					post_id = line['id']
					link = None

					try:
						link = hyper_links.find({'_id':url})

					except:	
						print 'shiz its not there'
						
					if link == None:
						print 'link == None'
						insert_link = {'_id':url,'count':1, 'posts':[post_id]}
						hyper_links.insert(insert_link)

					else:
						
						hyper_links.update({'_id':url},{'$inc':{'count':1}, '$addToSet':{'posts':post_id}}, upsert=True)



if __name__ == "__main__":
    main()
