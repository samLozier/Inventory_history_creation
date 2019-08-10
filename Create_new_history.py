import pyodbc
import time
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

import config as cfg

driver = cfg.mssql_server['driver']
server = cfg.mssql_server['server']
database = cfg.mssql_server['database']
uid = cfg.mssql_server['uid']
pwd = cfg.mssql_server['pwd']
con_string = f'DRIVER={driver};SERVER={server};UID={uid};PWD={pwd};DATABASE={database};'


def get_unique_styles():
    cnxn = pyodbc.connect(con_string)
    sql2 = """SET NOCOUNT ON;
            SELECT
            TB_STYLES.style_id
            FROM 
            TB_STYLES
            WHERE 
            TB_STYLES.BRAND = 'MSR'
            ;"""
    unique_styles = pd.read_sql(sql2, cnxn)
    unique_styles = unique_styles['style_id'].values.tolist()
    cnxn.close()
    return unique_styles

def run_style_audit(style):
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    print('run SP')
    params = (100, style, 1)
    sql = '''EXEC [dbo].[SP_STYLE_AUDIT] @IMACHINE=?, @ISTYLE=?, @VSTORE=?'''
    cursor.execute(sql, params)
    cursor.commit()
    cnxn.close()

    time.sleep(.5)

    cnxn = pyodbc.connect(con_string)
    print('fetch results')
    sql2 = """SET NOCOUNT ON;
        select 
        *
        from 
        tb_style_audit
        where tb_style_audit.machine_id = 100
        ;"""
    df_data = pd.read_sql(sql2, cnxn)
    cnxn.close()
    return df_data

def get_qoh(sku_bucket_id):
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    print('connected')
    sql = f'''select tb_sku_buckets.qoh, TB_SKU_LOOKUPS.LOOKUP as upc
        from tb_sku_buckets
        INNER JOIN TB_SKU_LOOKUPS on TB_SKU_LOOKUPS.SKU_ID = TB_SKU_BUCKETS.SKU_ID
        where tb_sku_buckets.sku_bucket_id = {sku_bucket_id}
        and TB_SKU_LOOKUPS.PRIME = 'Y';'''
    response = cursor.execute(sql)
    for row in response:
        qoh = row.qoh
        upc = row.upc.strip()
        return qoh, upc


def add_quantities(df_just_quantity, qoh):  # add qoh arguement to determine end date
    # DONE = sum the item change per day eg. +1 -1 = 0 change that day
    # DONE = insert a row above the oldest date with qoh = 0
    # take qoh and use it as a starting count for quantity. it's the only known, non relative value
    # append it as a new row with todays date
    # create a new column and do the addition and subtraction back up to the original date - it should equal 0
    # index on the date and then fill in missing date values, fill down the QOH column with the last seen QOH
    min_date = min(df_just_quantity['DTE'])
    max_date = max(df_just_quantity['DTE'])
    new_row_min = pd.DataFrame({'DTE': min_date - timedelta(1), 'QTY': 0}, index=[0])
    new_row_max = pd.DataFrame({'DTE': max_date + timedelta(1), 'QTY': (qoh * -1)}, index=[0])
    df_just_quantity = pd.concat([new_row_min, df_just_quantity]).reset_index(drop=True)
    df_just_quantity = pd.concat([new_row_max, df_just_quantity]).reset_index(drop=True)
    df_just_quantity = df_just_quantity.set_index(pd.to_datetime(df_just_quantity['DTE']))
    df_resample = pd.DataFrame()
    df_resample['QTY'] = df_just_quantity.QTY.resample('D').sum()
    df_resample['QTY'] = df_resample[['QTY']].mul(-1, axis=1)
    df_resample['DailyOH'] = df_resample.QTY[::-1].cumsum()
    df_resample.drop(['QTY'], axis=1, inplace=True)
    return df_resample


def get_totals_for_style(style):
    for sku_bucket_id in style['SKU_BUCKET_ID'].unique():
        qoh, upc = get_qoh(sku_bucket_id)
        quantity = add_quantities(style, qoh)
        quantity['upc'] = upc
        quantity['store'] = 1
        return quantity


def insert_to_pgdb(full_totals):
    user = cfg.pg_server['user']
    password = cfg.pg_server['password']
    host = cfg.pg_server['host']
    database = cfg.pg_server['dbname']
    port = cfg.pg_server['port']
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}', echo=False)
    full_totals.to_sql(name='inventory', con=engine, if_exists='append', index=True)
    print('insert successful')


## do work


style_list = get_unique_styles()
for style in tqdm(style_list):
    audit_results = run_style_audit(style)
    full_totals = get_totals_for_style(audit_results)
    print('inserting into pg')
    if full_totals is not None:
        insert_to_pgdb(full_totals)
    else:
        continue
