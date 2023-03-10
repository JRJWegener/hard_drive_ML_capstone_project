import polars as pl
import pandas as pd
import numpy as np



def create_faildate_dict(df):
    """create dictionary with the serial numbers and the corresponding date at which the hard drive failed

    Args:
        df (polars DataFrame): DataFrame from which the datapoints with failures should be extracted

    Returns:
        dictionary: dictionary with serial_number as keys and dates as values
    """
    # get the data entries at the dates of failure
    df_new = df.filter(pl.col("failure") == 1)
    #transform to pandas DataFrame and get the serial number and date
    df_new = df_new.to_pandas()
    df_new = df_new.loc[:,["serial_number", "date"]]
    # transform dataframe to dictionary
    df_index = df_new.set_index("serial_number", drop=True)
    fail_dict = df_index.to_dict()["date"]
    return fail_dict

def create_faildate(df, fail_dict):
    """Function to map the date at which the Hard drive will fail to each Data Point with the corresponding serial number

    Args:
        df (polars DataFrame): DataFrame to which the new column should be added
        fail_dict (dictionary): dictionary with key: serial number and value: date (YYYY-mm-dd)

    Returns:
        polars DataFrame: DataFrame with the new  column 'faildate' added
    """
    #map the dictionary to the DataFrame
    df_with_faildates = df.with_columns(pl.col("serial_number").map_dict(fail_dict).alias("faildate"))
    #cast the Dates into datetime format
    df_with_faildates = df.with_columns(pl.col(['date','faildate']).str.strptime(pl.Date, fmt='%Y-%m-%d'), strict=False)
    df_with_faildates.drop_in_place("strict")
    return df_with_faildates

def create_target_classification(df):
    """Function to create a new column with classification of 1 if failure happens in the next 14 days or 0 if no failure happens in the next 14  days

    Args:
        df (polars DataFrame): DataFrame with the date of failure in the column 'faildate'

    Returns:
        Polars DataFrame: DataFrame with new column 'failing_in14days' (binary classification)
    """
    # calculate time until failure
    df_with_faildates = df.with_columns((pl.col('faildate')- pl.col('date')).alias('Time_till_failure'))
    #create new column with classification based on time until failire
    df_with_target = df_with_faildates.with_columns(
    pl.when(pl.col("Time_till_failure") >= pl.duration(days=14)).then(0).\
        when(pl.col("Time_till_failure") == None).then(0).\
            otherwise(1).alias("failing_in14days"))
    return df_with_target

def search_faulty_drives(df):
    """Function to search for hard drive serial numbers with entries after their failure date. Saves the result to a csv file for later use

    Args:
        df (polars DataFrame): DataFrame with calculated time until failure

    """
    faulty_drives = df.filter(pl.col('Time_till_failure') < 0 )['serial_number'].unique()
    # save results to csv
    np.savetxt("Faulty_drives.csv", 
           faulty_drives.to_list(),
           delimiter =", ", 
           fmt ='% s')
    print('faulty drives saved to: Faulty_drives.csv')


def feature_creation(year=2022):
    df_all = pl.read_parquet(f'./data/{year}_data_selected.parquet')
    fail_dict = create_faildate_dict(df_all)
    df_date = create_faildate(df_all, fail_dict)
    df_classified = create_target_classification(df_date)
    df_classified.write_parquet(f'./data/{year}_data_selected.parquet')
    print("")
    search_faulty_drives(df)



if __name__ == "__main__":
    feature_creation()