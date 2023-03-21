import polars as pl
import pandas as pd
from sklearn.model_selection import train_test_split

def feature_list(target=True):
    """creates a list of columns which should be used for transforming

    Returns:
        col_list: list of columns for importing
    """
    #number of the smart features to be imported
    number_list = [1,3,4,5,7,9,10,12,187,188,192,193,194,197,198,199]
    if target == True:
        col_list = ["target"]
    else:
        col_list = []
    for n in number_list:
    #   string1 = f"smart_{n}_normalized"
        string2 = f"smart_{n}_raw"
    #   col_list.append(string1)
        col_list.append(string2)
    return col_list

def timeseries_batches(df, window=14):
    """splits up a timeseries into batches of a specified window size

    Args:
        df (polars DataFrame): time-series of a single hard drive
        window (int, optional): the size of the batches the time series should be split into. Defaults to 14.
    Returns:
        list_df: list of DataFrames
    """
    length = len(df)       
    leftover = length % window
    df_sorted = df.sort(by="date").to_pandas()
    #cut df down to multiples of the window size (cutoff happens at beginning)
    if leftover != 0:
        if length <= 13:
                return None
        else:
            df_cut = df_sorted.loc[leftover: , :]
    else:
        df_cut = df_sorted
    #create batches of window size as a list of dataframes
    list_df = [df_cut[i:i+window] for i in range(0,len(df_cut),window)]
    for l in list_df:
        l.reset_index(inplace=True, drop=True)
    return(list_df)


def concat_batches(df_rocket, list_df):
    lower_limit = len(df_rocket.index.levels[0])
    upper_limit = lower_limit + len(list_df)
    df_batches = pd.concat(list_df, axis=0, keys=range(lower_limit, upper_limit))
    df_return = pd.concat([df_rocket, df_batches], axis=0)
    return df_return


def transform_rocket(list_df,df_rocket):
    """transform batches of time series in a list of DataFrames into a DataFrame format for sktime rocket.

    Args:
        list_df (list of DataFrames): time series batches in a list
        targets (pandas Series): series of classification targets for the time series batches
        df_rocket (pandas DataFrame): DataFrame to which the time series should be added (in sktime rocket format)
    Returns:
        df_rocket(pandas DataFrame): DataFrame with new row with time series from the list of DataFrames.
    """
    features = feature_list(target=False)
    for df in list_df:
        feature_dict = {}
        for f in features:
            feature_dict[f] = list((df[f]).reset_index(drop=True))
        if 1 in df["failing_in14days"].unique():
            feature_dict["target"] = 1
        else: 
            feature_dict["target"] = 0
        df_rocket = df_rocket.append(feature_dict, ignore_index = True)
    df_rocket["target"] = df_rocket["target"].astype(int)
    return(df_rocket)


def get_serial(df, modelnumber="ST4000DM000"):
    serial_numbers = df.filter(pl.col("model") == modelnumber)["serial_number"].unique()
    return serial_numbers


def transform_all(dataframe, serial_numbers,df_rocket):

    counter = 0
    op_length = len(serial_numbers)
    for s in serial_numbers:
        df_serial = dataframe.filter(pl.col("serial_number") == s)
        list_df = timeseries_batches(df_serial)
        if list_df == None:
            continue
        df_rocket = concat_batches(df_rocket, list_df)
    #    df_rocket = transform_rocket(list_df,df_rocket)
        if counter % 100 == 0:
            print(f"{counter} of {op_length}")
        counter += 1
    return df_rocket


#def batchwise(dataframe, serial_numbers,df_rocket):
#    slicer = 1500
#    batches = []
#    for d in range(1, divider):

def filter_out_inconsistent_drives(df):
    inconsistent_drives = pd.read_csv("./data/Faulty_drives.csv", header=None)
    df_new = df.filter(~pl.col("serial_number").is_in(list(inconsistent_drives[0])))
    return df_new

def custom_train_test_split(df, modelnumber="ST4000DM000"):
    serial_numbers = get_serial(df, modelnumber)
    df_serials = pd.DataFrame(list(serial_numbers))
    df_failing = df.filter(pl.col("failure") == 1)
    failing_serials = df_failing["serial_number"].unique()
    df_serials["fail"] = df_serials[0].apply(lambda x: 1 if x in failing_serials else 0)
    Train, Test = train_test_split(df_serials, stratify=df_serials["fail"])
    return list(Train[0]), list(Test[0])

def create_y(df):
    y = []
    for instance in df.index.levels[0]:
        if 1.0 in list(df.loc[(instance),"failing_in14days"]):
            y.append(1)
        else: 
            y.append(0)
    return y

def divide_chunks(l, n):
     
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

def create_chunks(df, serial_numbers, name, chunksize=1500):
    chunks = divide_chunks(serial_numbers, chunksize)
    counter = 1
    for chunk in chunks:
        my_index = pd.MultiIndex(levels=[[],[]],codes=[[],[]], names=["instances","timepoints"])
        df_rocket = pd.DataFrame(columns= feature_list(target=False), index= my_index)
        df_rocket = transform_all(df,chunk, df_rocket)
        df_rocket.to_parquet(f"./data/datachunks/{name}_chunk{counter}.parquet")
        counter += 1


if __name__ == "__main__":
    pass
