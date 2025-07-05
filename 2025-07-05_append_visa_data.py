"""
/usr/bin/env python
coding: utf-8
Author: Tiangeng Lu

Objectives
 - Plain code version of https://github.com/tiangenglu/automation/blob/main/2025-04-08_append_visa_data.ipynb
 - Built upon previous work [2023-06-30_cleaning_df_bothdata.py](https://github.com/tiangenglu/WebScrape/blob/main/06302023_cleaning_df_bothdata.py)
 - Detect new raw data and ONLY process the newly scraped data
 - After cleaning, append to the existing all-time visa data

 - Last executed on 2025-07-04
 - Recommend test run when new scraped data come in
"""

import pandas as pd
import boto3
import json
import io
import sys

with open("aws_credential.txt", 'r') as file:
    aws_credential=json.load(file)
s3=boto3.Session(
    profile_name = None, 
    region_name = 'us-east-2').client(
    's3',
    aws_access_key_id=aws_credential['access_key'],
    aws_secret_access_key=aws_credential['secret_key'])

output_content=s3.list_objects(Bucket = aws_credential['bucket'], Prefix ='visa_output/')['Contents']
output_items = [d['Key'].split('/')[-1] for d in output_content]

time_iv_bytes=s3.get_object(Bucket = aws_credential['bucket'], 
              Key = 'visa_output/time_iv.txt')['Body'].read()
time_iv=pd.read_csv(io.BytesIO(time_iv_bytes), delimiter = "\t", header = None)

time_niv_bytes=s3.get_object(Bucket = aws_credential['bucket'], 
              Key = 'visa_output/time_niv.txt')['Body'].read()
time_niv=pd.read_csv(io.BytesIO(time_niv_bytes), delimiter = "\t", header = None)

print("Most recent NIV processed:",time_niv.max()[0],"\nMost recent IV processed:",time_iv.max()[0])



bucket_niv_content=s3.list_objects(Bucket = aws_credential['bucket'], Prefix ='messy_data/visa_scraped/niv/')['Contents']
bucket_iv_content=s3.list_objects(Bucket = aws_credential['bucket'], Prefix ='messy_data/visa_scraped/iv/')['Contents']



bucket_niv_items=[d['Key'].split('/')[-1] for d in bucket_niv_content]
bucket_iv_items=[d['Key'].split('/')[-1] for d in bucket_iv_content] 


bucket_iv_date=[f.split('.')[0].split('_')[-1] for f in bucket_iv_items]
bucket_niv_date=[f.split('.')[0].split('_')[-1] for f in bucket_niv_items]


print("Most recent IV scraped:",bucket_iv_date[-1],"\nMost recent NIV scraped:", bucket_niv_date[-1])


len_diff_niv=len(bucket_niv_items) - len(time_niv)
if len_diff_niv > 0:
    print(f'Need to append {len_diff_niv} month(s) to existing NIV data.')
else: print("No actions required, compiled NIV data is up to date.")



len_diff_iv=len(bucket_iv_items) - len(time_iv)
if len_diff_iv > 0:
    print(f'Need to append {len_diff_iv} month(s) to existing IV data.')
else: print("No actions required, compiled IV data is up to date.")



if len_diff_iv == 0 and len_diff_niv == 0:
    print("No actions required, compiled NIV and IV data are up to date.")
    sys.exit(0)
else: print("Proceed with workflow.")


niv_date_to_add=bucket_niv_date[-len_diff_niv:]
print(f'NIV needs the following month(s): {niv_date_to_add}')
iv_date_to_add = bucket_iv_date[-len_diff_iv:]
print(f'IV needs the following month(s): {iv_date_to_add}')



niv_df_raw = [None] * len_diff_niv
for i in range(len_diff_niv):
    print(f'Getting the {-(i+1)} item from the .txt folder:')
    file = s3.get_object(Bucket = aws_credential['bucket'],
              # offset zero indexing: -(i+1), start from most recent (last in)
              Key = 'messy_data/visa_scraped/niv/'+bucket_niv_items[-(i+1)])['Body'].read()
    #niv_df_raw[i] = pd.DataFrame(file.decode("utf-8").split('\n')) # this works
    niv_df_raw[i] = pd.read_csv(io.BytesIO(file), delimiter = "\t", header = None) # also works


iv_df_raw = [None] * len_diff_iv
for i in range(len_diff_iv):
    print(f'Getting the {-(i+1)} item from the .txt folder:')
    file = s3.get_object(Bucket = aws_credential['bucket'],
              # offset zero indexing: -(i+1), start from most recent (last in)
              Key = 'messy_data/visa_scraped/iv/'+bucket_iv_items[-(i+1)])['Body'].read()
    #niv_df_raw[i] = pd.DataFrame(file.decode("utf-8").split('\n')) # this works
    iv_df_raw[i] = pd.read_csv(io.BytesIO(file), delimiter = "\t", header = None) # also works
print(len(iv_df_raw))


niv_grand_total = [] # an empty to store grand totals
for i in range(len(niv_df_raw)):
    niv_df_raw[i].columns = ['V'] # more robust than .rename
    # or use .apply(lambda x: x.strip()), but the following is simple
    niv_df_raw[i]['V']=niv_df_raw[i]['V'].str.strip().str.upper()
    # insert an iterrows() loop to get the index of the grand total row
    for idx,row in niv_df_raw[i].iterrows():
        if 'grand total'.upper() in row['V']:
            niv_grand_total.append(row)
            idx_rm_below = idx
    niv_df_raw[i]=niv_df_raw[i].iloc[:idx_rm_below]
    # offset zero indexing: -(i+1), start from most recent
    niv_df_raw[i]['time'] = niv_date_to_add[-(i+1)]


gt=pd.DataFrame(niv_grand_total)['V'].str.replace('grand total'.upper(),'').str.replace(',','')
niv_total=pd.DataFrame(data={'time':niv_date_to_add[::-1],'total':gt})
niv_total=niv_total.sort_values(by = 'time').reset_index(drop = True)
niv_total['total'] = niv_total['total'].astype(int)
del gt
print(niv_total)


iv_grand_total = []
for i in range(len(iv_df_raw)):
    iv_df_raw[i].columns = ['V']
    iv_df_raw[i]['V']=iv_df_raw[i]['V'].str.strip().str.upper()
    for idx,row in iv_df_raw[i].iterrows():
        if 'grand total'.upper() in row['V']:
            iv_grand_total.append(row)
            # index for the grand total row, all rows below grand total will be excluded
            idx_rm_below = idx
    iv_df_raw[i] = iv_df_raw[i].iloc[:idx_rm_below]
    iv_df_raw[i]['time'] = iv_date_to_add[-(i+1)]



# When the loop is completed,create a small dataframe to store monthly grand totals
gt=pd.DataFrame(iv_grand_total)['V'].str.replace('grand total'.upper(),'').str.replace(',','')
iv_total=pd.DataFrame(data={'time':iv_date_to_add[::-1],'total':gt})
iv_total=iv_total.sort_values(by = 'time').reset_index(drop = True)
iv_total['total'] = iv_total['total'].astype(int)
del gt
print(iv_total)


df_raw_niv=pd.concat([df for df in niv_df_raw])
df_niv = df_raw_niv.copy(deep = True)


df_raw_iv=pd.concat([df for df in iv_df_raw])
df_iv=df_raw_iv.copy(deep=True)


niv_headers = ['NONIMMIGRANT','NATIONALITY VISA','\\(FY', '\\#SBU','PAGE','SENSITIVE']
'|'.join([h for h in niv_headers])


df_niv_headers=df_niv[df_niv['V'].str.contains('|'.join(niv_headers))]



df_niv=df_niv.iloc[~df_niv.index.isin(df_niv_headers.index)]


df_iv['V'] = df_iv['V'].str.strip()
df_iv = df_iv[df_iv['V'].str.len() > 1]
iv_headers = ['PAGE ', 'FOREIGN STATE OF', 'CHARGEABILITY', 
              'PLACE OF BIRTH', '\\(FY 20', '\\(FY20',
              'IMMIGRANT VISA', 'SENSITIVE']
df_iv_headers = df_iv.loc[df_iv['V'].str.contains('|'.join(iv_headers))]
df_iv = df_iv.iloc[~df_iv.index.isin(df_iv_headers.index)]


# get rid of the warning messages
pd.options.mode.copy_on_write = True
df_niv['nationality']=[' '.join(row.split(' ')[:-2]).strip() for row in df_niv['V']]
# visa class
df_niv['visa']=[row.split(' ')[-2].strip() for row in df_niv['V']]
# remove thousand separator , from numbers
df_niv['issue']=[row.split(' ')[-1].replace(',','').strip() for row in df_niv['V']]


df_iv['nationality']=[' '.join(row.split(' ')[:-2]).strip() for row in df_iv['V']]
df_iv['visa']=[row.split(' ')[-2].strip() for row in df_iv['V']]
df_iv['issue']=[row.split(' ')[-1].replace(',','').strip() for row in df_iv['V']]


# Are there non-numeric values in the visa issuance count column?
check_numeric=[s for s in df_niv['issue'] if not str(s).isdigit()]
if len(check_numeric)>0:
    print("At least one row has non-numeric values in the NIV issuance column. Go back and check.")
    print(check_numeric)
    sys.exit()
else:
    print("No non-numeric values were detected in the NIV issuance column. Good to proceed.")
    df_niv['issue'] = df_niv['issue'].astype(int)


# Are there non-numeric values in the visa issuance count column?
check_numeric=[s for s in df_iv['issue'] if not str(s).isdigit()]
if len(check_numeric)>0:
    print("At least one row has non-numeric values in the IV issuance column. Go back and check.")
    print(check_numeric)
    sys.exit()
else:
    print("No non-numeric values were detected in the IV issuance column. Good to proceed.")
    df_iv['issue'] = df_iv['issue'].astype(int)


restore_idx = []
# here's how any() works
for idx,row in df_niv_headers.iterrows():
    if any(c in row['V'] for c in df_niv.nationality.unique()):
        print(idx, row)
        restore_idx.append(idx)


df_restore=df_niv_headers.loc[restore_idx]


df_restore['nationality']=[' '.join(row.split('NONIMMIGRANT')[0].strip().split(' ')[:-2]).strip() for row in df_restore['V']]
df_restore['visa'] = [row.split('NONIMMIGRANT')[0].strip().split(' ')[-2].strip() for row in df_restore['V']]
df_restore['issue'] = [row.split('NONIMMIGRANT')[0].strip().split(' ')[-1].strip().replace(',','') for row in df_restore['V']]
df_restore['issue'] = df_restore['issue'].astype(int)
print(df_restore)


restore_idx_iv = []
# here's how any() works
for idx,row in df_iv_headers.iterrows():
    if any(c in row['V'] for c in df_iv.nationality.unique()):
        print(idx, row)
        restore_idx_iv.append(idx)

df_restore_iv=df_iv_headers.loc[restore_idx_iv]
df_restore_iv['nationality']=[' '.join(row.split('IMMIGRANT')[0].strip().split(' ')[:-2]).strip() for row in df_restore_iv['V']]
df_restore_iv['visa'] = [row.split('IMMIGRANT')[0].strip().split(' ')[-2].strip() for row in df_restore_iv['V']]
df_restore_iv['issue'] = [row.split('IMMIGRANT')[0].strip().split(' ')[-1].strip().replace(',','') for row in df_restore_iv['V']]
df_restore_iv['issue'] = df_restore_iv['issue'].astype(int)
print(df_restore_iv)



col_order = ['nationality', 'visa', 'issue','time']


df_niv=pd.concat([df_niv, df_restore]).sort_index().drop(columns = ['V'])[col_order].drop_duplicates().reset_index(drop=True)
df_iv=pd.concat([df_iv, df_restore_iv]).sort_index().drop(columns=['V'])[col_order].drop_duplicates().reset_index(drop=True)
df_niv = df_niv.rename(columns={'issue':'count'})
df_iv = df_iv.rename(columns={'issue':'count'})




niv_calculate_total=df_niv.groupby(['time'])[['count']].sum().reset_index()
# niv_total is the published total, created before cleaning
niv_compare=niv_calculate_total.merge(niv_total, on='time')
niv_compare['diff'] = niv_compare['count'] - niv_compare['total']
del niv_calculate_total

print(niv_compare)


iv_calculate_total=df_iv.groupby(['time'])[['count']].sum().reset_index()
iv_compare = iv_calculate_total.merge(iv_total,on='time')
iv_compare['diff'] = iv_compare['count'] - iv_compare['total']
del iv_calculate_total
print(iv_compare)



niv_alltime=s3.get_object(Bucket = aws_credential['bucket'], 
              Key = 'visa_output/df_niv.csv')['Body'].read()
df_niv_alltime = pd.read_csv(io.BytesIO(niv_alltime),low_memory=True)




iv_alltime=s3.get_object(Bucket = aws_credential['bucket'], 
              Key = 'visa_output/df_iv.csv')['Body'].read()
df_iv_alltime = pd.read_csv(io.BytesIO(iv_alltime),low_memory=True)




df_niv_alltime_new=pd.concat([df_niv_alltime, df_niv]).reset_index(drop=True).drop_duplicates()
df_iv_alltime_new=pd.concat([df_iv_alltime, df_iv]).reset_index(drop=True).drop_duplicates()




df_niv_alltime_new=df_niv_alltime_new.sort_values(by=['time','nationality','visa']).reset_index(drop = True)
df_iv_alltime_new=df_iv_alltime_new.sort_values(by=['time','nationality','visa']).reset_index(drop = True)



country_list=list(set(list(df_iv_alltime_new['nationality'].unique()) + 
                      list(df_niv_alltime_new['nationality'].unique()
                          )
                     )
                 )
print("Total unique country/nationality labels before cleaning: ",len(country_list))


special_chars = []
# instead of iterrows, can also work on a list of unique nationalities
for country in country_list:
    for char in country:
        if not (char.isalpha() or char == ' '):
            if char not in special_chars:
                special_chars.append(char)
                print(char, country)



special_chars.remove(',') # potential legit
special_chars.remove("'") # potential legit


old_country_label = []
new_country_label = []
for country in country_list:
    # the following covers the case when one string contains multiple special characters: e.g., '(' and ')'
    if any(char in country for char in special_chars):
        # if it's a letter or a space, join as usual, then replace special character with a space
        new_country = ''.join([char if (char.isalpha() or char == ' ') 
                               else char.replace(char,' ') 
                               for char in country]) # replace special character with space
        new_country = ' '.join(new_country.split()).replace('BORN','').strip() # split() to remove excessive space
        old_country_label.append(country)
        new_country_label.append(new_country)
        print("\nold: ",country,'\nnew: ', new_country)



no_sp_char_label=dict(zip(old_country_label,new_country_label))


# map new country labels to a new column nationality2, then replace it with original nationality
df_niv_alltime_new['nationality2'] = df_niv_alltime_new['nationality'].map(
    no_sp_char_label).fillna(
    df_niv_alltime_new['nationality'])
df_iv_alltime_new['nationality2'] = df_iv_alltime_new['nationality'].map(
    no_sp_char_label).fillna(
    df_iv_alltime_new['nationality'])
df_niv_alltime_new = df_niv_alltime_new.drop(
    columns=['nationality']).rename(
    columns={'nationality2':'nationality'})
df_iv_alltime_new = df_iv_alltime_new.drop(
    columns=['nationality']).rename(
    columns={'nationality2':'nationality'})



country_list_new=list(set(list(df_iv_alltime_new['nationality'].unique()) + 
                      list(df_niv_alltime_new['nationality'].unique()
                          )
                     )
                 )
print("Total unique country/nationality labels after cleaning: ",len(country_list_new))
print(f'After removing special characters, {len(country_list) - len(country_list_new)} labels were reduced.')



with open('country_list.txt','w') as file:
    file.write('\n'.join(country_list_new))
new_col_order = ['nationality','visa', 'count', 'time']
df_niv_alltime_new = df_niv_alltime_new[new_col_order]
df_iv_alltime_new = df_iv_alltime_new[new_col_order]

time_niv=sorted(list(df_niv_alltime_new['time'].unique()))
time_iv=sorted(list(df_iv_alltime_new['time'].unique()))

# output a list of time stamps
with open('time_niv.txt', 'w') as file:
    file.write('\n'.join(time_niv))
with open('time_iv.txt', 'w') as file:
    file.write('\n'.join(time_iv))

df_niv_alltime_new.to_csv('df_niv.csv',index=False)
df_iv_alltime_new.to_csv('df_iv.csv',index=False)


# make list a string to upload
s3.put_object(Body = "\n".join([c for c in country_list_new]), 
              Bucket = aws_credential['bucket'], 
              Key = 'visa_output/country_list.txt')
s3.put_object(Body = "\n".join([t for t in time_niv]), 
              Bucket = aws_credential['bucket'], 
              Key = 'visa_output/time_niv.txt')
s3.put_object(Body = "\n".join([t for t in time_iv]), 
              Bucket = aws_credential['bucket'], 
              Key = 'visa_output/time_iv.txt')
# list objects after upload
output_folder_items=s3.list_objects(Bucket = aws_credential['bucket'], Prefix = 'visa_output')['Contents']
item_names=[d['Key'] for d in output_folder_items]
print([item for item in item_names if item.endswith('.txt')])


# upload pandas dataframe to s3
csv_buffer = io.StringIO()
df_niv_alltime_new.to_csv(csv_buffer, index=False)
s3.put_object(Body = csv_buffer.getvalue(), 
              Bucket = aws_credential['bucket'], 
              Key = 'visa_output/df_niv.csv')
# create a new csv buffer object to upload a different data frame
csv_buffer = io.StringIO()
df_iv_alltime_new.to_csv(csv_buffer, index=False)
s3.put_object(Body = csv_buffer.getvalue(), 
              Bucket = aws_credential['bucket'], 
              Key = 'visa_output/df_iv.csv')
# list objects after upload
output_folder_items=s3.list_objects(Bucket = aws_credential['bucket'], Prefix = 'visa_output')['Contents']
item_names=[d['Key'] for d in output_folder_items]
print([item for item in item_names if item.endswith('.csv')])