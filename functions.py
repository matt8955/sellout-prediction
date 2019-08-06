import requests
import pandas as pd
import numpy as np
import sqlalchemy as db
import keys
import json
import datetime
import string

def scrape_ticketmaster(size, page_num):
    '''takes in size=num of requests and which page num you are scraping
    and pulls returns a df that will need to be cleaned'''
    url = 'https://app.ticketmaster.com/discovery/v2/events.json?countryCode=US'
    req = requests.get(f'{url}&apikey={keys.ticketmaster}&size={size}&page={page_num}')
    x = json.loads(req.text)
    df = pd.DataFrame(x['_embedded']['events'])
    return df

def drop_columns(df):
    '''drops the columns in the df not needed for data analysis'''
    df = df.copy
    df.drop(columns = ['id','images','pleaseNote','test','info',
    'accessibility', 'locale', 'promoter','type','_links', 'seatmap',
    'products', 'dates', 'outlets', 'promoters'],inplace=True)
    return df

def unpack_classifications(df):
    '''unpacks the json classifications and adds petitinent columns to df'''
    df = df.copy
    df['genre'] = df.classifications.apply(lambda x: x[0]['genre']['name'])
    df['subgenre'] = df.classifications.apply(lambda x: x[0]['subGenre']['name'])
    df.drop(columns = ['classifications'],inplace = True)
    return df

def unpack_presales(df):
    '''unpacks json presale column returns a df'''
    df = df.copy
    sales_df = pd.io.json.json_normalize(df.sales)
    sales_df.presales.fillna(0,inplace = True)
    sales_df['presales'] = sales_df['presales'].apply(lambda x: 1 if x!= 0 else 0)
    #drop na (not onsale yet) cant merge!!!
    # sales_df.dropna(inplace = True)
    sales_df.drop(columns=['public.startTBD'], inplace = True)
    sales_df['public.startDateTime'] = pd.to_datetime(sales_df['public.startDateTime'])
    sales_df['public.endDateTime'] = pd.to_datetime(sales_df['public.endDateTime'])
    #need to merge
    df.drop(columns=['sales'],inplace=True)
    return df.merge(sales_df, on=df.index)

def unpack_price(df):
    '''unpacks price column returns a df'''
    #unpack price
    df = df.copy
    df.priceRanges.fillna(1, inplace=True) # if no pricerange json in cell then event sold out
    df['sold_out'] = df.priceRanges.apply(lambda x: x if x==1 else 0)
    df['price_min'] = df.priceRanges.apply(lambda x: x[0]['min'] if x !=1 else np.nan)
    df['price_max'] = df.priceRanges.apply(lambda x: x[0]['max'] if x !=1 else np.nan)
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
    df = df.copy
    df.ticketLimit.fillna('arb',inplace=True)
    df['max_tickets'] = df.ticketLimit.apply(lambda x: ticket_limit(x) if x !='arb' else x)
    df['max_tickets'] = df.max_tickets.apply(lambda x: x if is_numeric(x) else 'arb')
    df.max_tickets.replace('arb', np.nan, inplace=True)
    df.drop(columns = ['ticketLimit'], inplace=True)
    return df

def unpack_venue(df):
    df = df.copy()
    venue_df = pd.io.json.json_normalize(df['_embedded'].apply(lambda x: x['venues'][0]))
    venue_df = venue_df[['name','postalCode','markets','city.name', 'state.name',
    'state.stateCode', 'country.countryCode', 'address.line1',
    'location.longitude', 'location.latitude']]
    df = df.join(venue_df)
    return df

def num_of_markets(x):
    try:
        return len(x)
    except:
        return x

def unpack_market(df):
    df = df.copy()
    df['num_markets'] = df.markets.apply(lambda x: num_of_markets(x))
    df.drop(columns = ['markets'],inplace=True)
    return df

def final_cleanup(df):
    '''clean up the rest of the dataframe'''
    df = df.copy()
    df.drop(columns = ['state'],inplace = True)
    df.columns = ['event_name', 'url', 'genre', 'subgenre', 'is_presale',
    'onsale_date','event_date', 'is_sold_out', 'price_min', 'price_max',
    'max_tickets','venue_name', 'postalCode', 'city', 'state','country',
    'address', 'longitude', 'latitude','num_markets']
    return df
