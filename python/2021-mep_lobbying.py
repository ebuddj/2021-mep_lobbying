#!/usr/bin/python
# -*- coding: UTF8 -*-
# @See http://www.python.org/dev/peps/pep-0263/

#######
# ABOUT
#######

# MEP lobbying data web crawler

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
      data[item.tag] = item.text
    db_meps.insert_one(data)
else:
  print('\033[1mUsing existing mep data from db.\033[0m\n')

#######################
# FETCH MEETINGS DATA #
#######################

# Import beautiful soup for parsing html document
from bs4 import BeautifulSoup

db_meetings.delete_many({})
for mep in db_meps.find({}):
  page = 1
  meeting_id = 0
  while True:
    # See https://www.europarl.europa.eu/meps/en/124726/HENNA_VIRKKUNEN/meetings/past#detailedcardmep
    url = 'https://www.europarl.europa.eu/meps/en/loadmore-meetings/past/' + mep['id'] + '?slice=' + str(page)
    # Henna Virkkunen
    url = 'https://www.europarl.europa.eu/meps/en/loadmore-meetings/past/124726?slice=' + str(page)
    req = Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
    content = urlopen(req)
    soup = BeautifulSoup(content.read(), 'html.parser')
    if (len(soup.find_all(class_ = 'erpl_meps-activity')) > 0):
      data = {}
      for meeting in soup.find_all(class_ = 'erpl_meps-activity'):
        data['topic'] = meeting.find(class_ = 't-item').getText().strip()
        data['time'] = meeting.find('time').getText().strip()
        [x.extract() for x in meeting.select('time')]
        data['location'] = ' '.join(meeting.find(class_ = 'erpl_subtitle').getText().split()).replace('- ', '').replace('-', '')
        if (meeting.find(class_ = 'erpl_badge')):
          data['committee'] = meeting.find(class_ = 'erpl_badge').getText().strip()
          [x.extract() for x in meeting.select('span.badges')]
        else:
          data['committee'] = ''
        data['rapporteur'] = ' '.join(meeting.find(class_ = 'erpl_report').getText().split())
        data['meeting_id'] = meeting_id
        data['mep_id'] = '124726'
        
        for lobbyist in meeting.find(class_ = 'erpl_rapporteur').getText().strip().split(','):
          data['lobbyist'] = lobbyist.strip()
          data['_id'] = ObjectId() 
          db_meetings.insert_one(data)
        
        meeting_id = meeting_id + 1

      print ('Page' + str(page) + ' ... done!')
      page = page + 1
    else: 
      break
  break


# with open('clean/00_' + file_type + '.csv', 'wb') as csvfile:
#   csv_writer = csv.writer(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
#   csv_writer.writerow([''])
#   csv_writer.writerow([mep['name'].encode('utf-8')])