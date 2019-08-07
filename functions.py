import requests
import pandas as pd
import numpy as np
import sqlalchemy as db
import keys
import json
import datetime
import string

def scrape_ticketmaster(state, size, page_num):
    '''takes in size=num of requests and which page num you are scraping
    and pulls returns a df that will need to be cleaned'''
    fails = []
    url = 'https://app.ticketmaster.com/discovery/v2/events.json?countryCode=US&classificationName=music'
    req = requests.get(f'{url}&apikey={keys.ticketmaster}&size={size}&page={page_num}&stateCode={state}')
    x = json.loads(req.text)
    try:
        df = pd.DataFrame(frame['_embedded']['events'])
        clean_frames.append(df)
    except:
        fails.append(frame)
    return df

def drop_nontm(df):
    df.copy()
    df['url'] = df.url.apply(lambda x: x if x[12:24] == 'ticketmaster' else np.nan)
    df.url.dropna(inplace=True)
    return df

def drop_columns(df):
    '''drops the columns in the df not needed for data analysis'''
    df = df.copy()
    try:
        df.ticketLimit
    except:
        df['ticketLimit'] = np.nan
    try:
        df.priceRanges
    except:
        df['priceRanges'] = np.nan
    df= df[['name', 'url', 'locale', 'sales', 'dates', 'classifications', 'priceRanges', 'ticketLimit', '_embedded']]
    return df
def try_apply(x, col):
    try: 
        return x[0][col]['name']
    except:
        return np.nan
        
def unpack_classifications(df):
    '''unpacks the json classifications and adds petitinent columns to df'''
    df = df.copy()
    df['genre'] = df.classifications.apply(lambda x: try_apply(x, 'genre'))
    df['subgenre'] = df.classifications.apply(lambda x: try_apply(x, 'subGenre'))
    df.drop(columns = ['classifications'],inplace = True)
    return df

def unpack_presales(df):
    '''unpacks json presale column returns a df'''
    df = df.copy()
    sales_df = pd.io.json.json_normalize(df.sales)
    try:
        sales_df.presales
    except:
        sales_df['presales'] = np.nan
    sales_df.presales.fillna(0,inplace = True)
    sales_df['presales'] = sales_df['presales'].apply(lambda x: 1 if x!= 0 else 0)
    #drop na (not onsale yet) cant merge!!!
    # sales_df.dropna(inplace = True)
    sales_df.drop(columns=['public.startTBD'], inplace = True)
    try:
        sales_df['public.startDateTime'] = pd.to_datetime(sales_df['public.startDateTime'])
        sales_df['public.endDateTime'] = pd.to_datetime(sales_df['public.endDateTime'])
    except:
        sales_df
    #need to merge
    df.drop(columns=['sales'],inplace=True)
    df = df.merge(sales_df, on=df.index)
    df.drop(columns=['key_0'], inplace=True)
    return df

def try_minmax(x, s):
    try:
        return x[0][s] if x !=1 else np.nan
    except:
        return np.nan
        

def unpack_price(df):
    '''unpacks price column returns a df'''
    #unpack price
    df = df.copy()
    df['priceRanges'].fillna(1, inplace=True) # if no pricerange json in cell then event sold out
    df['sold_out'] = df.priceRanges.apply(lambda x: x if x==1 else 0)
    df['price_min'] = df.priceRanges.apply(lambda x: try_minmax(x,'min'))
    df['price_max'] = df.priceRanges.apply(lambda x: try_minmax(x, 'max'))
    df.drop(columns=['priceRanges'],inplace=True)
    return df

def ticket_limit(x):
    try:
        return x['info'].split('(', 1)[1].split(')')[0]
    except:
        return 'arb'

def is_numeric(s):
    if s[0] in string.digits:
        return True
    else:
        False

def unpack_limit(df):
    '''unpacks ticketlimit column to return df'''
    df = df.copy()
    df.ticketLimit.fillna('arb',inplace=True)
    df['max_tickets'] = df.ticketLimit.apply(lambda x: ticket_limit(x) if x !='arb' else x)
    df['max_tickets'] = df.max_tickets.apply(lambda x: x if is_numeric(x) else 'arb')
    df.max_tickets.replace('arb', np.nan, inplace=True)
    df.drop(columns = ['ticketLimit'], inplace=True)
    return df

def unpack_venue(df):
    df = df.copy()
    venue_df = pd.io.json.json_normalize(df['_embedded'].apply(lambda x: x['venues'][0]))
    try:
        venue_df.markets
    except:
        venue_df['markets'] = np.nan
    venue_df = venue_df[['name','postalCode','markets','city.name', 'state.name',
    'state.stateCode', 'country.countryCode', 'address.line1',
    'location.longitude', 'location.latitude']]
    df = df.merge(venue_df, on=df.index)
    df.drop(columns=['key_0'], inplace=True)
    return df

def num_of_markets(x):
    try:
        return len(x)
    except:
        return x

def unpack_market(df):
    df = df.copy()
    try:
        df.markets
    except:
        df['markets'] = np.nan
    df['num_markets'] = df['markets'].apply(lambda x: num_of_markets(x))
    df.drop(columns = ['markets'],inplace=True)
    return df

def final_cleanup(df):
    '''clean up the rest of the dataframe'''
    df = df.copy()
    df.drop(columns = ['_embedded', 'state.name','locale','dates'],inplace = True)
    df.rename(columns = {'name_x' : 'event_name','public.startDateTime' : 'onsale_date', 
          'public.endDateTime' : 'event_date', 'presales' : 'is_presale',
          'name_y' : 'venue_name','city.name' : 'city', 
          'state.stateCode': 'state', 'country.countryCode': 'country',
          'address.line1' : 'address', 'location.longitude' : 'longitude',
          'location.latitude' : 'latitude'}, inplace=True)
    return df

def to_sql(df, table, engine):
    '''appends db to named sql table'''
    df = df.copy()
    df.to_sql(name = 'events',if_exists='append',con = engine)
