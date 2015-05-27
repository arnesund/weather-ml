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
DAYS = 3

# Places to query the API for
places = ['Norway/Oslo', 'Norway/Stavanger', 'Norway/Kristiansand',
          'Norway/Bergen', 'Norway/Trondheim']

# Output directories and filenames
OUTDIR = 'data'
OUTFILE = 'dataset.csv'
LOGFILE = 'logs/generate_dataset.log'

# Wunderground API details
API_BASEURL = 'http://api.wunderground.com/api/'
API_PRODUCT = 'history'

# Initialize logger with debug log to disk and info messages to console
logger = logging.getLogger('generate_dataset')
logger.setLevel(level=logging.DEBUG)
fh = logging.FileHandler(LOGFILE)
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(fh)
logger.addHandler(ch)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# Read API key from environment
if os.environ.has_key('WUNDERGROUND_APIKEY'):
    API_KEY = os.environ.get('WUNDERGROUND_APIKEY')
else:
    logger.error('Environment variable WUNDERGROUND_APIKEY not set!')
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

# Prepare output destinations
for place in places:
    path = os.path.join(OUTDIR, place)
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            logger.error('Unable to create output path: ' + \
                '{0}, aborting!'.format(path))
            sys.exit(1)

# Generate ascending list of dates to query for (format: YYYYMMDD)
dates = []
today = datetime.date.today()
for d in xrange(DAYS, 0, -1):   # Start DAYS days in the past and go forward
    dates.append(datetime.date.strftime(today - datetime.timedelta(days=d), \
        "%Y%m%d"))

data = {}
for place in places:
    for date in dates:
        logger.info('Processing {0} date {1}...'.format(place, date))

        # Get data from Wunderground API
        try:
            res = requests.get(API_BASEURL + '/' + API_KEY + '/' + \
                    API_PRODUCT + '_' + str(date) + '/q/' + place + '.json')
            res_data = res.json()
        except:
            logger.error('Unable to query API for {0} date {1}, skipping it.'.format(place, date))
            continue
        
        # Save JSON data to one file per date per place
        try:
            out = open(os.path.join(OUTDIR, place, date) + '.json', 'w')
            pprint(res_data, out)
            out.close()
        except:
            logger.error('Unable to write data to output file for ' + \
                '{0} date {1}, skipping it.'.format(place, date))
            continue

        # Add data to dictionary
        for obs in res_data['history']['observations']:
            # Find UTC date and time
            d = obs['utcdate']['year'] + obs['utcdate']['mon'] + \
                obs['utcdate']['mday']
            t = obs['utcdate']['hour'] + ':' + obs['utcdate']['min']

            # Initialize data storage for date, time and place
            if d not in data:
                data[d] = {}
            if t not in data[d]:
                data[d][t] = {}
            if place not in data[d][t]:
                data[d][t][place] = []

            # Save observation to list, in the order specified by obs_fields
            for field in obs_fields:
                if field in obs:
                    data[d][t][place].append(obs[field])
                else:
                    data[d][t][place].append('')


# Open CSV writer for final output
try:
    out = open(os.path.join(OUTDIR, OUTFILE), 'wb')
    csvout = csv.writer(out, delimiter=',')
except:
    logger.error('Unable to open output file for writing, aborting!')
    sys.exit(1)

# Generate header
header = [u'date', u'utctime']
for place in places:
    # Prefix all field names with first char of city name (after the slash)
    offset = place.find('/')
    prefix = place[offset+1:offset+2].lower()
    for field in obs_fields:
        header.append('_'.join([prefix, field]))

# Save header to file
csvout.writerow(header)

# Process all observations and write them to CSV file
for d in data:
    for t in data[d]:
        obs = []

        # Validate dataset
        if len(data[d][t].keys()) != len(places):
            # Skip this datetime, since ML needs all observations 
            # to have the same amount of fields
            logger.warning('Datetime {0}-{1} skipped. '.format(d, t) + \
                'Does not have data for all places ({2} out of {3}).'.format(\
                len(data[d][t].keys()), len(places)))
            continue

        # Concatenate all observations into one long list
        for place in places:
            obs = obs + data[d][t][place]

        # Save observation to output file
        csvout.writerow([d, t] + obs)

# Close output file
out.close()
