import requests, datetime
import pandas as pd

utc_now = datetime.datetime.now()

#1 - Initialising variables and data structures for use with the API

base_url = 'https://data.police.uk/api/crimes-at-location'
#See https://data.police.uk/changelog/ for latest state of crime data refresh, usually 3 months lag for most areas.
get_api_locations = {'572809':'Crewe','699411':'Mistley', '793830':'Hatfield', '880870':'Wigston', '929681':'Surbiton', '955110':'Beckton (1)', '962370':'Beckton (2)','979396':'Wealdstone'}

def get_api_dates(utc_now):
    #Get the completed months of this year
    set_api_months = [m for m in range(1,utc_now.month)]
    #Create row labels in the format "YYYY-MM"
    set_api_dates = ["{}-{}".format(utc_now.year,m) for m in set_api_months]
    return set_api_dates

#2 - Initalising objects to be used in for loop

df_cols = ['month','category','area']
maindf = pd.DataFrame(columns=df_cols)
empty = []

#3 - The ETL operations

#EXTRACT - json from data.police.uk API using HTTP GET
for date in get_api_dates(utc_now):
    for location in get_api_locations:
        set_url_filter = ['date={}'.format(date), 'location_id={}'.format(location)] 
        get_url_filter = str.join("&", set_url_filter)
        
        with requests.get(base_url, params=get_url_filter, timeout=10) as api_response:                       
            status = api_response.status_code
            payload = api_response.content.decode()
#TRANSFORM - add column to data set           
            if status is 200 and payload is not empty:
                #Convert JSON payload into pandas object
                subdf = pd.DataFrame(pd.read_json(payload), columns = ('month', 'category'))
                #Add a column 'area'. Obtain value of location_id key in 'get_api_locations'
                subdf.insert(2, "area", get_api_locations[location])
                #Append this dataframe to output dataframe
                maindf = maindf.append(subdf, ignore_index = True)
                del subdf
                print(maindf)
            else:
                print("no data for {} / {}".format(date,api_locations[location]))
                print(maindf)
#LOAD - to csv file which is created in the local directory
maindf.to_csv('police_api_output_'+str(utc_now.strftime('%Y-%m-%d'))+'.csv', encoding='utf-8',index=False, header=True, mode="w")
print(maindf)
