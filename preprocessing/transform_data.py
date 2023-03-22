import polars as pl
import pandas as pd
from sklearn.model_selection import train_test_split
from warnings import warn

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
    """attaches batches of time-series to a pandas DataFrame

    Args:
        df_rocket (pandas DataFrame): DataFrame to which the batches should be attached
        list_df (List of pandas DataFrames): list of DataFrames with one time series per DF

    Returns:
        pandas DataFrame: DataFrame with the attached time-series
    """
    lower_limit = len(df_rocket.index.levels[0])
    upper_limit = lower_limit + len(list_df)
    df_batches = pd.concat(list_df, axis=0, keys=range(lower_limit, upper_limit))
    df_return = pd.concat([df_rocket, df_batches], axis=0)
    return df_return


def transform_rocket(list_df,df_rocket):
    """transform batches of time series in a list of DataFrames into a DataFrame format for sktime rocket.
        This function is deprecated and not used anymore!
    Args:
        list_df (list of DataFrames): time series batches in a list
        targets (pandas Series): series of classification targets for the time series batches
        df_rocket (pandas DataFrame): DataFrame to which the time series should be added (in sktime rocket format)
    Returns:
        df_rocket(pandas DataFrame): DataFrame with new row with time series from the list of DataFrames.
    """
    warn('This function is deprecated.', DeprecationWarning, stacklevel=2)
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
    """get a Series with all serial numbers in a DataFrame matching a specified model number

    Args:
        df (polars DataFrame): DataFrame containing hard drive records
        modelnumber (str, optional): Name of the model for which serial numbers should be found. Defaults to "ST4000DM000".

    Returns:
        series: a series containing the matched serial numbers
    """
    serial_numbers = df.filter(pl.col("model") == modelnumber)["serial_number"].unique()
    return serial_numbers


def transform_all(dataframe, serial_numbers,df_rocket):
    """transforms a subset of records defined by serial numbers to a sktime compatible format. The format used is: Time series panels - "pd-mutliindex" 
    (see: https://github.com/sktime/sktime/blob/main/examples/AA_datatypes_and_datasets.ipynb)

    Args:
        dataframe (polars DataFrame): DataFrame with records
        serial_numbers (series): a series listing the serial numbers of the records that should be transformed
        df_rocket (pandas DataFrame): DataFrame to which transformed time series should be concatenated.

    Returns:
        pandas DataFrame: DataFrame with added transformed time series
    """
    counter = 0
    op_length = len(serial_numbers)
    for s in serial_numbers:
        df_serial = dataframe.filter(pl.col("serial_number") == s)
        df_serial = df_serial.drop_nulls(feature_list(target=False))
        list_df = timeseries_batches(df_serial)
        if list_df == None:
            continue
        df_rocket = concat_batches(df_rocket, list_df)
    #    df_rocket = transform_rocket(list_df,df_rocket)
    #    if counter % 100 == 0:
    #        print(f"{counter} of {op_length}")
        counter += 1
    return df_rocket


def filter_out_inconsistent_drives(df):
    """Function to filter out records of hard drives which show inconsistent time series. Uses a previously created .csv file with the serial numbers of hard drives that showed inconsistent time series.

    Args:
        df (polars DataFrame): DataFrame with records that should be filtered.

    Returns:
        polars DataFrame: DataFrame with filtered records
    """
    inconsistent_drives = pd.read_csv("./data/Faulty_drives.csv", header=None)
    df_new = df.filter(~pl.col("serial_number").is_in(list(inconsistent_drives[0])))
    return df_new

def custom_train_test_split(df, modelnumber="ST4000DM000", RSEED=42):
    """function to split dataset of hard drive records into train and test set. splits the data by putting the serial numbers in different lists.
    Makes sure the distribution of failures is the same in both sets by stratifying according to failure states of hard drives.

    Args:
        df (polars DataFrame): DataFrame which should be split
        modelnumber (str, optional): name of the model which should be used in the train-test-split incase several are in the data set. Defaults to "ST4000DM000".
        RSEED (int, optional): random seed to be used for the split. Defaults to 42.

    Returns:
        list: list of serial numbers to be used for train set
        list: list of serial numbers to be used for test set

    """
    serial_numbers = get_serial(df, modelnumber)
    df_serials = pd.DataFrame(list(serial_numbers))
    df_failing = df.filter(pl.col("failure") == 1)
    failing_serials = df_failing["serial_number"].unique()
    df_serials["fail"] = df_serials[0].apply(lambda x: 1 if x in failing_serials else 0)
    Train, Test = train_test_split(df_serials, stratify=df_serials["fail"], random_state=RSEED)
    return list(Train[0]), list(Test[0])

def create_y(df):
    """creates series with binary classification for each instance in the given DataFrame

    Args:
        df (pandas DataFrame): DataFrame with multiindexing and target encoded in column 'failing_in14days'

    Returns:
        pandas Series: Series with target for each instance (0 = no target, 1 = target)
    """
    y_df = df.iloc[::14, :]["failing_in14days"].astype(int).reset_index(drop=True)
    return y_df

def divide_chunks(series_list, chunk_size):
    """function to generate chunks of a specified size from a list

    Args:
        series_list (list): list of serial numbers
        chunk_size (int): size of the chunks in which the list should be partitioned

    Yields:
        generator: generator of chunks
    """
    # looping till length of list
    for i in range(0, len(series_list), chunk_size):
        yield series_list[i:i + chunk_size]

def create_chunks(df, serial_numbers, name, chunksize=1500):
    """splits and transforms a DataFrame into chunks with sktime compatible format Time series panels - "pd-mutliindex" (see: https://github.com/sktime/sktime/blob/main/examples/AA_datatypes_and_datasets.ipynb)


    Args:
        df (polars DataFrame): DataFrame which should be split up
        serial_numbers (list): list of serial numbers which should be transformed
        name (str): name which should be used in the filenames of the saved chunks. should fit into this scheme: '{name}_chunk{number}.parquet'
        chunksize (int, optional): size of the chunks that should be saved. Defaults to 1500.
    """
    chunks = divide_chunks(serial_numbers, chunksize)
    chunklist = list(chunks)
    chunk_len = len(chunklist)
    counter = 1
    for chunk in chunklist:
        my_index = pd.MultiIndex(levels=[[],[]],codes=[[],[]], names=["instances","timepoints"])
        df_rocket = pd.DataFrame(columns= feature_list(target=False), index= my_index)
        df_rocket = transform_all(df,chunk, df_rocket)
        df_rocket.to_parquet(f"./data/datachunks/{name}_chunk{counter}.parquet")
        print(f"{counter}. chunk out of {chunk_len} processed and saved!")
        counter += 1

def combine_chunks(name , chunknumber):
    """create a pandas DataFrame out of saved chunks (parquet files)

    Args:
        name (str): identifier used in in the naming of the chunks. should fit into this scheme: '{name}_chunk{number}.parquet'
        chunknumber (int): number of saved chunks that should be concatenated

    Returns:
        pandas DataFrame: DataFrame out of concatenated chunks
    """
    df = pd.DataFrame()
    for i in range(1,chunknumber):
        df_new = pd.read_parquet(f"data/datachunks/{name}_chunk{i}.parquet")
        df = pd.concat([df, df_new])

    # reset indices of new DataFrame on first level
    # code courtesy of jezrael on stackoverflow (https://stackoverflow.com/questions/46445965/reset-a-recurring-multiindex-in-pandas)
    a = df.index.get_level_values(0).to_series()
    a = a.ne(a.shift()).cumsum() - 1
    new_index = pd.MultiIndex.from_arrays([a, df.index.get_level_values(1)], names=df.index.names)
    df.index = new_index    

    return df

if __name__ == "__main__":
    pass
