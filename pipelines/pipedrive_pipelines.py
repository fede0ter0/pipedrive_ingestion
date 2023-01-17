import pandas
from sqlalchemy import create_engine
import time

# Custom pluggins
from apis.pipedrive_client import DealsTimeLine, Deals, Persons
from loggers.logger import app_logger

# Logger
log = app_logger()

def fetch_persons_by_date(date):
    persons = Persons()
    resp_json = persons.get_data()

    persons_list = []
    for page_data in resp_json:
        persons_list.extend(page_data['data'])
    persons_df = pandas.json_normalize(persons_list)
    return persons_df

def fetch_deals_by_day(date):
    deals = DealsTimeLine()
    resp_json = deals.get_data(start_date=date, interval='day',amount=2,field_key='update_time')
    data = resp_json['data']
    deal_list = []
    for data_element in data:
        deal_list.extend(data_element['deals'])
    df = pandas.DataFrame(deal_list)
    if not df.empty:
        df['data_date'] = pandas.to_datetime(df['add_time']).dt.date
    return df

def fetch_all_deals():
    deals = Deals()
    resp_json = deals.get_data()
    all_deals_list = []

    for page_data in resp_json:
        all_deals_list.extend(page_data['data'])
    all_deals_df = pandas.json_normalize(all_deals_list)

    return all_deals_df

def run_process_deals_daily(date,deal_schema):
    # 1. Extract deals of the day 'date'
    log.info('Fetching deals by timeline from Pipedrive API Date: {date}'.format(date=date))
    deals_df = fetch_deals_by_day(date)

    # 2. Filter data
    deal_columns = list(deal_schema.keys())
    deals_filtered = deals_df[deal_columns]
    deals_filtered = deals_filtered.rename(columns=deal_schema)

    # 3. Load data into Postgres
    #user,password,host,port,dbname,schema = ()
    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"    
    engine = create_engine(conn_string)
    conn = engine.connect()
    log.info(f'Connection to {conn_string} created OK')
    # try:
    #     engine.execute(f""" delete from staging.pipedrive_deals_daily where purchase_event_ts::date = '{date}' """)
    # except Exception:
    #     log.error('Could not delete rows because the table does not exist.')
        
    try:
        deals_filtered.to_sql('pipedrive_deals_daily', con=conn, schema=schema, if_exists='replace', index=False, method='multi')
    except Exception as e:
        log.error(f'Insertion finished with error {e}')
        raise
    log.info(f'Table loaded to {dbname}.{schema}.pipedrive_deals_daily OK')
    conn.close()
    return deals_filtered

def run_process_all_deals():
    log.info('Fetching all deals from Pipedrive API')
    all_deals_df = fetch_all_deals()
    all_deals_df.to_csv('all_deals.csv', sep=';', header=True, index=False)
    return all_deals_df

def run_process_persons_daily(date,person_schema):
    # 1. Extract persons updates of a certain date
    log.info('Fetching persons updated today from Pipedrive API Date: {date}'.format(date=date))
    persons_df = fetch_persons_by_date(date)    
    log.info(f'Showing first two persons {persons_df.head(2)}')

    # 2. Filter data
    person_columns = list(person_schema.keys())
    persons_filtered = persons_df[person_columns]
    persons_filtered = persons_filtered.rename(columns=person_schema)
    log.info(f'Showing first two persons modified: {persons_filtered.head(2)}')

    # 3. Load data into Postgres
    #user,password,host,port,dbname,schema = ()
    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"    
    engine = create_engine(conn_string)
    conn = engine.connect()
    log.info(f'Connection to {conn_string} created OK')
    try:
        persons_filtered.to_sql('pipedrive_persons_daily', con=conn, schema=schema, if_exists='append', index=False, method='multi')
    except Exception as e:
        log.error(f'Finished with error {e}')
    log.info(f'Table loaded to {dbname}.{schema}.pipedrive_persons_daily OK')
    conn.close()
    return persons_df