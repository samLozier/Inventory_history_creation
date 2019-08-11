import pyodbc

import pandas as pd
from sqlalchemy import create_engine

import config as cfg

driver = cfg.mssql_server['driver']
server = cfg.mssql_server['server']
database = cfg.mssql_server['database']
uid = cfg.mssql_server['uid']
pwd = cfg.mssql_server['pwd']
con_string = f'DRIVER={driver};SERVER={server};UID={uid};PWD={pwd};DATABASE={database};'


def sales_table():
    cnxn = pyodbc.connect(con_string)
    sql2 = """set NOCOUNT ON 
        SELECT
        CONVERT(DATE, (TB_RECEIPT.DATE_TIME)) AS DATE_SOLD
        , TB_RECEIPT.STORE_ID as STORE_ID
        , rtrim(TB_SKU_LOOKUPS.LOOKUP) as LOOKUP
        , SUM(TB_RECEIPTLINe.QUANTITY) AS 'Net Quantity'
        , avg(tb_receiptline.price) AS PRICE
        , avg(TB_RECEIPTLINE.COST) as COST
        , sum(TB_RECEIPTLINE.QUANTITY * TB_RECEIPTLINE.PRICE) AS 'Net Price'
        , sum(TB_RECEIPTLINE.QUANTITY * (TB_RECEIPTLINE.PRICE - TB_RECEIPTLINE.COST)) as 'Net Profit'
        FROM TB_RECEIPT
        INNER JOIN TB_RECEIPTLINE ON TB_RECEIPTLINE.RECEIPT_NUM = TB_RECEIPT.RECEIPT_NUM
        INNER JOIN TB_SKU_BUCKETS ON TB_SKU_BUCKETS.SKU_BUCKET_ID = TB_RECEIPTLINE.SKU_BUCKET_ID
        inner join TB_SKUS on tb_skus.SKU_ID = TB_SKU_BUCKETS.SKU_ID
        INNER JOIN TB_SKU_LOOKUPS ON TB_SKU_LOOKUPS.SKU_ID = TB_SKU_BUCKETS.SKU_ID
        inner join TB_STYLES on TB_STYLES.STYLE_ID = TB_SKUS.STYLE_ID
        WHERE TB_RECEIPT.STORE_ID = '1'
        and (TB_STYLES.BRAND = 'La Sportiva' or tb_styles.BRAND = 'MSR')
        GROUP BY
        CONVERT(DATE, (TB_RECEIPT.DATE_TIME))
        , TB_RECEIPT.STORE_ID
        , TB_SKU_LOOKUPS.LOOKUP
        ORDER BY 
        CONVERT(DATE, (TB_RECEIPT.DATE_TIME))
        ;"""
    transaction_history = pd.read_sql(sql2, cnxn, index_col='DATE_SOLD')
    #  transaction_history = transaction_history.set_index(pd.to_datetime(transaction_history['DATE_SOLD']))
    cnxn.close()
    print(transaction_history)
    return transaction_history


def insert_to_pgdb(full_totals):
    user = cfg.pg_server['user']
    password = cfg.pg_server['password']
    host = cfg.pg_server['host']
    database = cfg.pg_server['dbname']
    port = cfg.pg_server['port']
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}', echo=False)
    full_totals.to_sql(name='sales', con=engine, if_exists='append', index=True)


salesfromsql = sales_table()
insert_to_pgdb(salesfromsql)
