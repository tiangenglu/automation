#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 22:29:21 2023

@author: Tiangeng Lu

0. NOT for initial scraping, but for database & catalog UPDATES
1. IV data
2. Compare the updated URL list to those in the existing catalog, download files from new URLs
3. Converting downloaded nonimmigrant visa .pdf documents to .txt
4. Updating catalog data
5. Similar to `06202023_iv.py`, but different in the following:
    a) This program converts newly-downloaded .pdf to .txt
    b) This program DOES NOT re-build the catalog. Rather, it appends new rows to the existing catalog.
    c) This is the IV version of `06282023_update_niv.py`.
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
import boto3 # aws s3 connection
import json # read in credentials
def dtime(file):
    return datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d, %A, %H:%M:%S")


# URL
main_url = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics/immigrant-visa-statistics/monthly-immigrant-visa-issuances.html'
main_html = requests.get(main_url).content
# object type is 'bytes'
type(main_html)
# select text, `main_selector` type is `selector.unified.Selector`, NOT subscriptable
main_selector = Selector(text = main_html)
# get urls that contain .pdf; this selector.xpath.extract() is important
all_links = main_selector.xpath('//*[contains(@href, ".pdf")]/@href').extract()
# examine the PATTERNS of these links, then subset them
print(all_links[:5])
# the following step is data/url-specific, select the urls that contain certain pattern
national_links = [link for link in all_links if "FSC" in link]
national_links = list(set(national_links))
print(len(national_links))

# Count the URLs in each year: 2017 should have 10 (from March), 2023 has 4 (up to April), and all other years should have 12
print("2017:",len([link for link in national_links if "2017" in link]))
print("2018:",len([link for link in national_links if "2018" in link]))
print("2019:",len([link for link in national_links if "2019" in link]))
print("2020:",len([link for link in national_links if "202020" in link])) # not a typo but by the actual links
print("2021:",len([link for link in national_links if "2021" in link]))
print("2022:",len([link for link in national_links if "2022" in link]))
print("2023:",len([link for link in national_links if "2023" in link]))
print("2024:",len([link for link in national_links if "2024" in link]))

# add prefix
prefix = 'https://travel.state.gov'
# list comprehension/generator to replace a loop over list elements
national_links = [prefix + link for link in national_links if link.startswith('/content')]


### Read-in Existing Catalog ###

# check whether the catalog dataframe exists
if 'catalog' in globals():
    print("Yes. The catalog data is already here.")
else:
    print("No. Import the catalog data now.")
    catalog = pd.read_csv('iv_catalog.csv')
    catalog['mmyy'] = pd.to_datetime(catalog['mmyy']).dt.date
    catalog['year'] = catalog['year'].astype(str)


### Does the newly updated url list contains more urls than the existing catalog? ###
if len(national_links) == len(catalog['url']):
    pass
elif len(national_links) > len(catalog['url']):
    print("There are more urls in the updated list.")
else:
    print("The catalog includes more links. Please double-check the updated URL list.")


# Which is(are) the extra links in the updated list?
# this works
new_links = list(set(national_links).difference(catalog['url']))
print(len(new_links))
print(new_links)

# added on 9/1/23 to stop if no new links are detected
# re-ran on 3/3/2024, 4 new links detected
if len(new_links) < 1:
    print("Stop executing because no new urls were detected.")
    sys.exit()
else:
    print("Continue")

# extract month and year
new_month = [link.split('/')[-1].split('%')[0].upper() for link in new_links]
new_year = [link.split('/')[-1].split('%')[1][2:] for link in new_links]

# add the current year, this is needed whenever we start a new year.
current_year = str(dt.date.today().year)

for item in new_month:
    if item in list(set(catalog['month'])):
        print("MONTH in existing month list")
    else:
        raise ValueError("Error in extracted MONTH")

for item in new_year:
    if item in list(set(catalog['year'])):
        print("YEAR in existing year list")
    elif item in current_year:
        print("YEAR in current year but not yet in existing list")
    else:
        raise ValueError("ERROR in extracted YEAR")

############ CONSTRUCTING NEW ENTRIES IN CATALOG #################    
df_new = pd.DataFrame({
    'url': new_links,
    'month': new_month,
    'year': new_year
    })
# convert mm-yyyy to time stamp in 3 steps
df_new['mmyy'] = df_new['year'].str.cat(df_new['month'], sep = '-')
df_new['mmyy'] = pd.to_datetime(df_new['mmyy'], errors = 'ignore') + MonthEnd()
df_new['mmyy'] = df_new['mmyy'].dt.date

# confirm the work directory
print(os.getcwd())
path = 'iv'
path_Exist = os.path.exists(path)
if not path_Exist:
    os.makedirs(path)
print("Now, does the path exist?", os.path.exists(path))

new_filenames = [None] * len(df_new)
for i in range(len(df_new)):
    new_filenames[i] = os.getcwd() + '/' + path + '/' + 'iv_' + str(df_new['mmyy'][i])[:10] + '.pdf'     
new_filenames
df_new['filename'] = new_filenames # create the local file names, not yet downloaded

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
path_txt = 'ivtxt'
path_Exist = os.path.exists(path_txt)
if not path_Exist:
    print("The txt files folder doesn't exist yet. Create one.")
    os.makedirs(path_txt)
    print(os.path.exists(path_txt))
else:
    print("The txt files folder already exists.")

## txt names ##
new_txtnames = ['iv_'+str(txt)+'.txt' for txt in df_new['mmyy']]

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
# sort the catalog by time order
catalog_updated = pd.concat([catalog, df_new], axis = 0).sort_values('mmyy').reset_index(drop = True)
catalog_updated.to_csv('iv_catalog.csv', index = False)

#### AWS S3 backup, added on 2025-04-08, still sick but slowly recovering
with open("aws_credential.txt", 'r') as file:
    aws_credential=json.load(file)
s3=boto3.Session(
    profile_name = None, 
    region_name = 'us-east-2').client(
    's3',
    aws_access_key_id=aws_credential['access_key'],
    aws_secret_access_key=aws_credential['secret_key'])

        
my_prefix = 'messy_data/visa_scraped/iv'
file_counts=len(s3.list_objects(Bucket = aws_credential['bucket'], Prefix = my_prefix)['Contents'])
print(f'Before backing up, the bucket currently has {file_counts} objects.')

TEXT_BODY = [None] * len(new_TEXT)
# the outer list of all downloaded files    
for index, element in enumerate(new_TEXT):
    # the inner list of pages of a given file
    TEXT_BODY[index] = "\n".join([page for page in element])
    s3.put_object(Body = TEXT_BODY[index], Bucket=aws_credential['bucket'], Key=('messy_data/visa_scraped/iv/' + new_txtnames[index]))
file_counts=len(s3.list_objects(Bucket = aws_credential['bucket'], Prefix = my_prefix)['Contents'])
print(f'After backing up, the bucket currently has {file_counts} objects.')
        
bucket_files=[d['Key'].split('/')[-1] for d in 
 s3.list_objects(Bucket = aws_credential['bucket'], 
                 Prefix = my_prefix)['Contents']]
print("Last few items in the bucket after updates: ",bucket_files[-5:])

