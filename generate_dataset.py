#!/usr/bin/env python
#
# Get observation data from the Wunderground API for a set of days and places
# Save as CSV to use as dataset for machine learning
#
import os
import csv
import sys
import errno
import logging
import requests
import datetime
from pprint import pprint

# Number of days to query API for
DAYS = 14

# Output directory and filename
OUTDIR = 'data'
OUTFILE = 'dataset.csv'

# Wunderground API details
API_BASEURL = 'http://api.wunderground.com/api/'
API_PRODUCT = 'history'

# Read API key from environment
if os.environ.has_key('WUNDERGROUND_APIKEY'):
    API_KEY = os.environ.get('WUNDERGROUND_APIKEY')
else:
    logging.error('Environment variable WUNDERGROUND_APIKEY not set!')
    sys.exit(1)

# Fields to save from each observation (excluding purely informative fields)
obs_fields = [u'conds',
 u'dewpti',
 u'dewptm',
 u'fog',
 u'hail',
 u'heatindexi',
 u'heatindexm',
 u'hum',
 u'precipi',
 u'precipm',
 u'pressurei',
 u'pressurem',
 u'rain',
 u'snow',
 u'tempi',
 u'tempm',
 u'thunder',
 u'tornado',
 u'visi',
 u'vism',
 u'wdird',
 u'wdire',
 u'wgusti',
 u'wgustm',
 u'windchilli',
 u'windchillm',
 u'wspdi',
 u'wspdm']

# Places to query the API for
places = ['Norway/Oslo', 'Norway/Stavanger', 'Norway/Kristiansand',
          'Norway/Bergen', 'Norway/Trondheim']

# Prepare output destinations
for place in places:
    path = os.path.join(OUTDIR, place)
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            logging.error('Unable to create output path: ' + \
                '{0}, aborting!'.format(path))
            sys.exit(1)

# Generate list of dates to query for (format: YYYYMMDD)
dates = []
today = datetime.date.today()
for d in xrange(1, DAYS+1):
    dates.append(datetime.date.strftime(today - datetime.timedelta(days=d), \
        "%Y%m%d"))

data = {}
for place in places:
    for date in dates:
        logging.info('Processing {0} date {1}...'.format(place, date))

        # Get data from Wunderground API
        try:
            res = requests.get(API_BASEURL + '/' + API_KEY + '/' + \
                    API_PRODUCT + '_' + str(date) + '/q/' + place + '.json')
            res_data = res.json()
        except:
            logging.error('Unable to query API for {0} date {1}, skipping it.'.format(place, date))
            continue
        
        # Save JSON data to one file per date per place
        try:
            out = open(os.path.join(OUTDIR, place, date) + '.json', 'w')
            pprint(res_data, out)
            out.close()
        except:
            logging.error('Unable to write data to output file for ' + \
                '{0} date {1}, skipping it.'.format(place, date))
            continue

        # Add data to dictionary
        if place not in data:
            data[place] = {}
        if date not in data[place]:
            data[place][date] = res_data['history']['observations']

        break
    break


# Open CSV writer for final output
try:
    out = open(os.path.join(OUTDIR, OUTFILE), 'wb')
    csvout = csv.writer(out, delimiter=',')
except:
    logging.error('Unable to open output file for writing, aborting!')
    sys.exit(1)

# Generate header
header = [u'place', u'date', u'utctime'] + obs_fields
csvout.writerow(header)

# Process all observations and write them to CSV file
for place in data:
    for date in data[place]:
        for obs in data[place][date]:
            # Initialize observation as list with place, date and time first
            observation = [place]
            d = obs['utcdate']['year'] + obs['utcdate']['mon'] + \
                obs['utcdate']['mday']
            t = obs['utcdate']['hour'] + ':' + obs['utcdate']['min']
            observation.append(d)
            observation.append(t)
            # Add all fields to list, with data or empty strings
            for field in obs_fields:
                if field in obs:
                    observation.append(obs[field])
                else:
                    observation.append('')
            # Save observation to output file
            csvout.writerow(observation)

# Close output file
out.close()
