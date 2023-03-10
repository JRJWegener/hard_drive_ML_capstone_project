import polars as pl
import pandas as pd
import datetime as dt
import numpy as np

def create_faildate_dict(df):
    df_new = df.filter(pl.col("failure") == 1)
    df_new = df_new.to_pandas()
    df_new = df_new.loc[:,["serial_number", "date"]]
    df_index = df_new.set_index("serial_number", drop=True)
    fail_dict = df_index.to_dict()["date"]
    return fail_dict

def create_faildate(df, fail_dict):
    df_with_faildates = df.with_columns(pl.col("serial_number").map_dict(fail_dict).alias("faildate"))
    return df_with_faildates

def create_target_classification(df):
    df_with_faildates = df.with_columns(pl.col(['date','faildate']).str.strptime(pl.Date, fmt='%Y-%m-%d'), strict=False)
    df_with_faildates = df_with_faildates.with_columns((pl.col('faildate')- pl.col('date')).alias('Time_till_failure'))
    df_with_target = df_with_faildates.with_columns(
    pl.when(pl.col("Time_till_failure") >= pl.duration(days=14)).then(0).\
        when(pl.col("Time_till_failure") == None).then(0).\
            otherwise(1).alias("failing_in14days"))
    return df_with_target

def search_faulty_drives(df_with_target):
    faulty_drives = df_with_faildates.filter(pl.col('Time_till_failure') < 0 )['serial_number'].unique()
    np.savetxt("Faulty_drives.csv", 
           faulty_drives.to_list(),
           delimiter =", ", 
           fmt ='% s')
    return 'faulty drives saved to: Faulty_drives.csv'