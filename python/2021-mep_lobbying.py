#!/usr/bin/python
# -*- coding: UTF8 -*-
# @See http://www.python.org/dev/peps/pep-0263/

#######
# ABOUT
#######

# MEP lobbying data web crawler
# See https://www.europarl.europa.eu/meps/en/124726/HENNA_VIRKKUNEN/meetings/past#detailedcardmep

########
# AUTHOR
########

# Teemo Tebest (teemo.tebest@gmail.com)

#########
# LICENSE
#########

# CC-BY-SA 4.0 EBU / Teemo Tebest

#######
# USAGE
#######

# python 2021-mep_lobbying

# Import request for adding headers to our request.
from urllib.request import Request, urlopen

# Import sys for reading arguments.
import sys

# Import pymongo and make a connection.
from bson.objectid import ObjectId
from pymongo import MongoClient
connection = MongoClient('localhost', 27017)
db = connection.mep_lobbying
db_meps = db.meps
db_meetings = db.meetings

##################
# FETCH MEP DATA #
##################
if (len(sys.argv) > 1 and sys.argv[1] == 'true'):
  print('\033[1mDownloading latest mep data from online.\033[0m\n')

  # Download meps file from Europarliament. 
  url = 'https://www.europarl.europa.eu/meps/en/full-list/xml'
  req = Request(url)
  # See: https://stackoverflow.com/questions/62278538/pd-read-csv-produces-httperror-http-error-403-forbidden/62278737#62278737
  req.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
  content = urlopen(req)

  # Clear database from any previous entries.
  db_meps.delete_many({})
  
  # Import XML parser.
  import xml.etree.ElementTree as ET

  # Loop through the meps and store them in a mongo collection.
  # https://www.edureka.co/blog/python-xml-parser-tutorial/
  meps = ET.parse(content).getroot()
  for mep in meps:
    data = {}
    for item in mep:
      # {'fullName': 'Christian EHLER', 'country': 'Germany', 'politicalGroup': "Group of the European People's Party (Christian Democrats)", 'id': '28226', 'nationalPoliticalGroup': 'Christlich Demokratische Union Deutschlands', 'fetched': '0', 'has_data': False}
      data[item.tag] = item.text
    data['fetched'] = '0'
    data['has_data'] = False
    db_meps.insert_one(data)
else:
  print('\033[1mUsing existing mep data from db.\033[0m\n')

#######################
# FETCH MEETINGS DATA #
#######################

# Import beautiful soup for parsing html document.
from bs4 import BeautifulSoup
# Import datetime for storing the fetched date time.
from datetime import datetime
# Import time to sleep
import time

# Limit which members we crawl by how old is the data or fetch data for all.
if (len(sys.argv) > 2): 
  query = {'fetched':{'$lt':sys.argv[2]}}
  print('Fetching data for ' + str(db_meps.count_documents(query)) + ' meps')
else:
  query = {'fetched':{'$lt':'1'}}
  # Henna Virkkunen 124726
  # Ville Niinistö 197802
  # Ska Keller 96734
  # Irene Tinagli 197591
  # query = {'id':'197591'}
  print('Fetching data for ' + str(db_meps.count_documents(query)) + ' meps')
fetched = 0

# Go through the meps on by one.
for mep in db_meps.find(query):
  # Remove all previous data because meetings don't have a unique identifier at source.
  db_meetings.delete_many({'mep_id': mep['id']})
  page = 1
  meeting_id = 0
  # Go through pages one by one as long as we have data.
  while True:
    url = 'https://www.europarl.europa.eu/meps/en/loadmore-meetings/past/' + mep['id'] + '?slice=' + str(page)
    req = Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
    content = urlopen(req)
    # Initialise content soup.
    soup = BeautifulSoup(content.read(), 'html.parser')
    # Loop through meetings of single page if there are any.
    if (len(soup.find_all(class_ = 'erpl_meps-activity')) > 0):
      data = {}
      for meeting in soup.find_all(class_ = 'erpl_meps-activity'):
        # Store the data in variable.
        data['topic'] = meeting.find(class_ = 't-item').getText().strip()
        data['date'] = meeting.find('time').getText().strip()
        [x.extract() for x in meeting.select('time')]
        data['location'] = ' '.join(meeting.find(class_ = 'erpl_subtitle').getText().split()).replace('- ', '').replace('-', '')
        if (meeting.find(class_ = 'erpl_badge')):
          data['committee'] = []
          for badge in meeting.find_all(class_ = 'erpl_badge'):
            data['committee'].append(badge.getText().strip())
          data['committee'] = ','.join(data['committee'])
          [x.extract() for x in meeting.select('span.badges')]
        else:
          data['committee'] = ''
        data['position'] = ' '.join(meeting.find(class_ = 'erpl_report').getText().split())
        data['meeting_id'] = mep['id'] + '_' + str(meeting_id)
        data['mep_id'] = mep['id']
        for lobbyist in meeting.find(class_ = 'erpl_rapporteur').getText().strip().split(','):
          data['lobbyist'] = lobbyist.strip()
          data['_id'] = ObjectId() 
          db_meetings.insert_one(data)
        # Make a meeting id.
        meeting_id = meeting_id + 1
      print ('Page' + str(page) + ' ... done!')
      page = page + 1
      # break
    # If done with pages or no meetings what so ever.
    else:
      db_meps.update_one({'id': mep['id']}, {'$set': { 'fetched': str(datetime.now())}})
      if (page > 1):
        fetched = fetched + 1
        print (mep['fullName'] + ' (' + mep['id'] + ') ... done!')
        db_meps.update_one({'id': mep['id']}, {'$set': { 'has_data': True}})
        # Wait a second before starting with the next one.
        time.sleep(1)
      else:
        print (mep['fullName'] + ' (' + mep['id'] + ') ... no data!')
        db_meps.update_one({'id': mep['id']}, {'$set': { 'has_data': False}})
      # Jump to next Mep
      break
  # if fetched >= 100:
    # break

# Export to csv.
with open('./../data/data.csv', 'w') as csvfile:
  # Import csv for writing csv.
  import csv
  csv_writer = csv.writer(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
  csv_writer.writerow(['mep_id', 'fullName', 'country', 'politicalGroup', 'nationalPoliticalGroup', 'meeting_id', 'topic', 'date', 'location','committee', 'position', 'lobbyist'])

  query = {}
  for mep in db_meps.find(query):
    for meeting in db_meetings.find({'mep_id':mep['id']}):
      # ['mep_id', 'fullName', 'country', 'politicalGroup', 'nationalPoliticalGroup', 'meeting_id', 'topic', 'date', 'location','committee', 'position', 'lobbyist']
      csv_writer.writerow([meeting['mep_id'], mep['fullName'], mep['country'], mep['politicalGroup'], mep['nationalPoliticalGroup'], meeting['meeting_id'], meeting['topic'], meeting['date'], meeting['location'], meeting['committee'], meeting['position'], meeting['lobbyist']])

