import json
import pymongo
import re
import unicodedata
import urlparse
from urlobject import URLObject



def main():

	true = True
	false = False


	connection = pymongo.Connection("mongodb://localhost", safe=True)

	db=connection.ADN
	friendfinder = db.friendfinder


	for i in range(0,1405):


		for item in open('/Users/chadsahlhoff/python/ADN/appnet-logger/bin/ADNLogs/'+ str(i) +'.json','r'):
			line = eval(item)

			try:
				user = line['user']

			except:
				break

			username = user['username']
			name = user['name']
			user_id = user['id']

			entities = line['entities']
			mentions = entities['mentions']
			friends = []

			if (len(mentions) != 0):
				for mention in mentions:
					friend_name = mention['name']

											

					if (friendfinder.find_one({'$and': [ { '_id': user_id },{'friends.friend':friend_name} ] }) != None):
						print 'updated the friend'
						try:
							friendfinder.update({ '$and': [ { '_id': user_id }, {'friends.friend': friend_name} ] }, {'$inc':{'friends.$.count':1}}, upsert=True)
										
						except:
							print '===========Failed to update the friend==================='


					elif(friendfinder.find_one({'_id':user_id}) != None):
						print'insert the friend'
						try:
							friendfinder.update({'_id': user_id }, {'$addToSet':{'friends':{'friend':friend_name, 'count': 1 } }}, upsert=True)

						except: 
							print '======================Failed to insert the friend================'


					else:
						print 'make new user'
						try:
							friendfinder.insert({'_id':user_id, 'username': username, 'name': name, 'friends':[{'friend':friend_name, 'count':1}]})

						except:
							print '============Failed to Insert================'





if __name__ == "__main__":
	main()


