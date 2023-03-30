import polars as pl
import glob


def importing(df=None, year=2022):
    """Imports daily hard drive records from csv files into a pandas DataFrame

    Args:
        df (pandas DataFrame): DataFrame into which the imported data should be merged. If None is given a new dataframe is created
        year (int, optional): Year of the records. Defaults to 2022.

    Returns:
        pandas DataFrame: DataFrame with the new data
    """
    if df==None:
        df = pl.DataFrame()
    quarters = ["Q1","Q2","Q3", "Q4"]
    for q in quarters:
        file_list = glob.glob(f'./data/data_{q}_{year}/*.csv')
        file_list.sort()
        for f in file_list:
            df_read = pl.read_csv(f)
            df = df.vstack(df_read)
            print(f"{f} is imported!")
        df.write_parquet(f"./data/2022_{q}_data.parquet")
        df = pl.DataFrame()

def create_parquet():
    path = "/Users/kelechijohn/Neuefische/hard_drive_ML_project/data/data_Q4_2022/*.csv"
    col_list = ['date', 'serial_number', 'model', 'capacity_bytes', 'failure',
    'smart_1_normalized',
    'smart_1_raw',
    'smart_3_normalized',
    'smart_3_raw',
    'smart_4_normalized',
    'smart_4_raw',
    'smart_5_normalized',
    'smart_5_raw',
    'smart_7_normalized',
    'smart_7_raw',
    'smart_9_normalized',
    'smart_9_raw',
    'smart_10_normalized',
    'smart_10_raw',
    'smart_12_normalized',
    'smart_12_raw',
    'smart_187_normalized',
    'smart_187_raw',
    'smart_188_normalized',
    'smart_188_raw',
    'smart_192_normalized',
    'smart_192_raw',
    'smart_193_normalized',
    'smart_193_raw',
    'smart_194_normalized',
    'smart_194_raw',
    'smart_197_normalized',
    'smart_197_raw',
    'smart_198_normalized',
    'smart_198_raw',
    'smart_199_normalized',
    'smart_199_raw']
    df = pl.DataFrame()

    for fname in glob.glob(path):
        print(fname)
        df_new = pl.read_csv(fname, columns= col_list)
        #df_select = df_new.select(col_list)
        df = pl.concat([df,df_new], rechunk=True)
    df.write_parquet('/Users/kelechijohn/Neuefische/hard_drive_ML_project/data/data_Q4_2022_selected.parquet')

create_parquet()
