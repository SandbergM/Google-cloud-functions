
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
import datetime
import numpy as np
import os


load_dotenv()  # take environment variables from .env.

client = bigquery.Client()

PROJECT_ID = os.getenv( 'PROJECT_ID' )


def get_ticker_data( ticker, company_name, interval, start_timestamp, end_timestamp ):

    try:    

        # Full url
        url = f''\
        f'https://query1.finance.yahoo.com/v7/finance/download/{ ticker }'\
        f'?period1={ int(start_timestamp) }'\
        f'&period2={ int(end_timestamp) }'\
        f'&interval={ interval }'\
        f'&events=history'\
        f'&includeAdjustedClose=true'

        # Get CSV
        res                 = pd.read_csv( url )
        # Fix col names
        
        res.columns         = [ col.replace(' ', '_').replace( '*', '' ).lower().strip() for col in res.columns ]
        
        # Add ticker and company_name as columns
        res                 = res.reindex([ 'ticker', 'company_name', *res.columns.tolist() ], axis = 1)
        res['ticker']       = ticker
        res['company_name'] = company_name

        # Remove prev saved days ( If found )
        start_dt            = datetime.datetime.fromtimestamp(int( start_timestamp ))
        res['date']         = pd.to_datetime(res['date'], format="%Y-%m-%d")
        res.drop(res[(res['date'] <= start_dt )].index, inplace=True)
        
        # Some 'volume's come as float for some reason
        res.dropna( inplace=True )
        res['volume']       = res['volume'].astype(np.int64)
        
        return res

    except Exception as e :
        return pd.DataFrame()


def get_tickers_data():
    """
    @return array
    """
    return [ dict( row ) for row in client.query( f" SELECT * FROM `{ PROJECT_ID }.stocks.tickers` " ).result() ]

def save_ticker_data_to_bq( df ):
    try:
        df.to_gbq(
            destination_table   = "stocks.1d_historical_data", 
            project_id          = PROJECT_ID,
            if_exists           = 'append',
            reauth              = False,
            chunksize           = 10000
        )
        return True
    except Exception as e:
        print(e)
        return False

def update_tickers_table( ticker, unix ):
    """
    @param String ticker
    @param Integer ticker
    """
    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('ticker', 'STRING', ticker),
            bigquery.ScalarQueryParameter('unix', 'INTEGER', int( unix ))
        ]
    )

    client.query( 
        query = f" UPDATE `{ PROJECT_ID }.stocks.tickers` SET unix = @unix WHERE ticker = @ticker ", 
        job_config=query_config
    ).result()

def run( request = None ):

    log( "Script starting" )
    log( f"Getting tickers" )

    tickers = get_tickers_data()

    for ticker in tickers:

        log( f"Fetching { ticker.get( 'company_name' ) }" )
        
        res = get_ticker_data(
            ticker = ticker.get( 'ticker' ),
            company_name = ticker.get( 'company_name' ),
            interval = '1d',
            start_timestamp = ticker.get( 'unix' ),
            end_timestamp = datetime.datetime.now().timestamp()
        )

        if len( res ):
            log( f"Saving ticker data { ticker.get( 'company_name' ) }" )
            if save_ticker_data_to_bq( res ):
                log( f"Updating ticker table { ticker }" )
                update_tickers_table( ticker.get( 'ticker' ), res['date'].max().timestamp() )

def log( msg ):
    """
    @param String msg
    """
    curr_datetime = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print( f"     ###     [{curr_datetime}] { msg }" )


if os.getenv( 'ENV' ) == 'dev':
    run()