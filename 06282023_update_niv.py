#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 24 19:41:12 2023

Last executed on 07/03/2025

@author: Tiangeng Lu

0. NOT for initial scraping, but for database & catalog UPDATES
1. NIV data
2. Compare the updated URL list to those in the existing catalog, download files from new URLs
3. Converting downloaded nonimmigrant visa .pdf documents to .txt
4. Updating catalog data
"""

import os
from PyPDF2 import PdfReader
from datetime import datetime
import datetime as dt
import time as tm
import pandas as pd
import requests
from scrapy import Selector
from pandas.tseries.offsets import MonthEnd
from urllib import request
import sys
# added on 2025-04-01 to connect to aws s3 buckets

import boto3 # aws s3 connection
import json # read in credentials

def dtime(file):
    return datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d, %A, %H:%M:%S")


### Get most recent URLs ####
main_url = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics/nonimmigrant-visa-statistics/monthly-nonimmigrant-visa-issuances.html'
all_links = Selector(text = requests.get(main_url).content).xpath('//*[contains(@href, "Class.pdf")]/@href').extract()
print(len(all_links))
links = [link for link in all_links if ("Nationality" in link or "nationality" in link or "Nationlity" in link)]

# Count the URLs in each year: 2017 should have 10 (from March), 2023 has 4 (up to April), and all other years should have 12
print("2017:",len([link for link in links if "2017" in link]))
print("2018:",len([link for link in links if "2018" in link]))
print("2019:",len([link for link in links if "2019" in link]))
print("2020:",len([link for link in links if "202020" in link])) # not a typo but by the actual links
print("2021:",len([link for link in links if "2021" in link]))
print("2022:",len([link for link in links if "2022" in link]))
print("2023:",len([link for link in links if "2023" in link]))
print("2024:",len([link for link in links if "2024" in link]))
print("2025:",len([link for link in links if "2025" in link]))

prefix = 'https://travel.state.gov'
links = [prefix + link for i,link in enumerate(links) if link.startswith('/content')]

### Read-in Existing Catalog ###

# check whether the catalog dataframe exists
if 'catalog' in globals():
    print("Yes. The catalog data is already here.")
else:
    print("No. Import the catalog data now.")
    catalog = pd.read_csv('niv_catalog.csv')
    catalog['mmyy'] = pd.to_datetime(catalog['mmyy']).dt.date

### Does the newly updated url list contains more urls than the existing catalog? ###
# the following doesn't work
# [link for link in links if link in catalog['url']]
if len(links) == len(catalog['url']):
    pass
elif len(links) > len(catalog['url']):
    print("There are more urls in the updated list.")
else:
    print("The catalog includes more links. Please double-check the updated URL list.")

# Which is(are) the extra links in the updated list?
# this works
new_links = list(set(links).difference(catalog['url']))
print(len(new_links))
print(new_links)

# added on 9/1/23 to stop if no new links are detected
if len(new_links) < 1:
    print("Stop executing because no new urls were detected.")
    sys.exit()
else:
    print("Continue")

# extract month and year

new_month = [link.split('/')[-1].split('%')[0].upper() for link in new_links]
new_year = [link.split('/')[-1].split('%')[1][2:] for link in new_links]
#new_year = [int(yr) for yr in new_year] # convert to int
print(f'New Year: {new_year}') # New Year: ['2023', '2023', '2023', '2024'], string
print(catalog['year'].unique()) # [2017 2018 2019 2020 2021 2022 2023], be aware of dtype!
print(f'The data type of year in the catalog dataframe is: {catalog.year[0].dtype}')

# check whether the extracted year and month information 

list(set(catalog['month']))
list(set(catalog['year']))
# The following prints out potential bad extractions. But I recommend the following loop in a workflow.
[item for item in new_month if item not in list(set(catalog['month']))]
[item for item in new_year if item not in list(set(catalog['year']))]

# add the current year, this is needed whenever we start a new year.
current_year = str(dt.date.today().year)
print(current_year)
print(type(current_year))

for item in new_month:
    if item in list(set(catalog['month'])):
        print("MONTH in existing month list")
    else:
        raise ValueError("Error in extracted MONTH")

# error raised on 3/3/24
for item in new_year:
    if item in list(set(catalog['year'].astype(str))): # added .astype(str) to match dtype of new_year
        print("YEAR in existing year list")
    elif item in current_year:
        print("YEAR in current year but not yet in existing list")
    else:
        raise ValueError("ERROR in extracted YEAR") # needs revision

############ CONSTRUCTING NEW ENTRIES IN CATALOG #################    
df_new = pd.DataFrame({
    'url': new_links,
    'month': new_month,
    'year': new_year # has to be string
    })
# convert mm-yyyy to time stamp in 3 steps
df_new['mmyy'] = df_new['year'].str.cat(df_new['month'], sep = '-') # has to be string
df_new['mmyy'] = pd.to_datetime(df_new['mmyy'], errors = 'ignore') + MonthEnd()
df_new['mmyy'] = df_new['mmyy'].dt.date

path = 'niv'
path_Exist = os.path.exists(path)
if not path_Exist:
    os.makedirs(path)
print("Now, does the path exist?", os.path.exists(path))

new_filenames = [None] * len(df_new)
for i in range(len(df_new)):
    new_filenames[i] = os.getcwd() + '/' + path + '/' + 'niv_' + str(df_new['mmyy'][i])[:10] + '.pdf'     
new_filenames
df_new['filename'] = new_filenames

new_status = [None]*len(df_new)
# check the download status for each filename
for i, k in enumerate(df_new['filename']):
    if os.path.isfile(k):
        new_status[i] = True
        print(dtime(k))
    else:
        new_status[i] = False
        request.urlretrieve(df_new['url'][i], df_new['filename'][i]) # download here
# update status        
for i, k in enumerate(df_new['filename']):
    if os.path.isfile(k):
        new_status[i] = True
        print(str(k.split('/')[-1]), "was downloaded at", dtime(k))

# add the download status in catalog
if len(new_status) == len(df_new):
    df_new['downloaded'] = new_status 
df_new['download_time'] = [dtime(pdf) for pdf in df_new['filename']]
############ DONE WITH ADDING NEW ROWS TO CATALOG #################

#### txt ####
path_txt = 'nivtxt'
path_Exist = os.path.exists(path_txt)
if not path_Exist:
    print("The txt files folder doesn't exist yet. Create one.")
    os.makedirs(path_txt)
    print(os.path.exists(path_txt))
else:
    print("The txt files folder already exists.")

## txt names ##
new_txtnames = ['niv_'+str(txt)+'.txt' for txt in df_new['mmyy']]

## convert pdf to txt and save ##
new_TEXT = [None] * len(df_new['filename'])
# outer loop: pdf files
for n, nth_pdf in enumerate(df_new['filename']):
    # create a reader object
    reader = PdfReader(nth_pdf)
    Pages = [None] * len(reader.pages)
    Text = [None] * len(reader.pages)
    # inner loop: pdf pages
    for i in range(len(reader.pages)):
        Pages[i] = reader.pages[i]
        Text[i] = Pages[i].extract_text()
    new_TEXT[n] = Text
# save all txt
# two layers of loops
for i, txt in enumerate(new_txtnames):
    file = open(path_txt + '/' + txt, 'w')
    for individual_page in new_TEXT[i]:
        file.write(individual_page + "\n")
    file.close()
print("The txt folder now has", str(len(os.listdir(path_txt))), "files as of", tm.strftime("%Y-%m-%d, %A, %H:%M"))


############# FINALLY, UPDATE THE CATALOG #####################
catalog_updated = pd.concat([catalog, df_new], axis = 0).sort_values('mmyy').reset_index(drop = True)
catalog_updated.to_csv('niv_catalog.csv', index = False)

## back up data to AWS S3, added on 2025-04-01, TO BE TESTED WHEN NEW DATA PUBLISHES

with open("aws_credential.txt", 'r') as file:
    aws_credential=json.load(file)
s3=boto3.Session(
    profile_name = None, 
    region_name = 'us-east-2').client(
    's3',
    aws_access_key_id=aws_credential['access_key'],
    aws_secret_access_key=aws_credential['secret_key'])

        
my_prefix = 'messy_data/visa_scraped/niv'
file_counts=len(s3.list_objects(Bucket = aws_credential['bucket'], Prefix = my_prefix)['Contents'])
print(f'Before backing up, the bucket currently has {file_counts} objects.')

## added on 2025-04-08, still sick but slowly recovering
TEXT_BODY = [None] * len(new_TEXT)
# the outer list of all downloaded files    
for index, element in enumerate(new_TEXT):
    # the inner list of pages of a given file
    TEXT_BODY[index] = "\n".join([page for page in element])
    s3.put_object(Body = TEXT_BODY[index], Bucket=aws_credential['bucket'], Key=('messy_data/visa_scraped/niv/' + new_txtnames[index]))
file_counts=len(s3.list_objects(Bucket = aws_credential['bucket'], Prefix = my_prefix)['Contents'])
print(f'After backing up, the bucket currently has {file_counts} objects.')
        
bucket_files=[d['Key'].split('/')[-1] for d in 
 s3.list_objects(Bucket = aws_credential['bucket'], 
                 Prefix = my_prefix)['Contents']]
print("Last few items in the bucket after updates: ",bucket_files[-5:])