import polars as pl
import glob


def create_column_list():
    """creates a list of columns for importing

    Returns:
        list: list of columns for importing
    """
    #number of the smart features to be imported
    number_list = [1,3,4,5,7,9,10,12,187,188,192,193,194,197,198,199]    
    col_list = ['date', 'serial_number', 'model', 'capacity_bytes', 'failure']
    for n in number_list:
        string1 = f"smart_{n}_normalized"
        string2 = f"smart_{n}_raw"
        col_list.append(string1)
        col_list.append(string2)
    return col_list


def create_parquet(year=2022):
    """function to combine daily records from csv files into quarterly records and save them to a parquet file.

    Args:
        year (int, optional): year from which records should be pulled. Defaults to 2022.
    """
    quarters = ["Q1", "Q2", "Q3", "Q4"]
    col_list = create_column_list()
    for q in quarters:
        path = f"./data/data_{q}_{year}/*.csv"
        
        df = pl.DataFrame()
        path = path.sort()
        for fname in glob.glob(path):
            print(fname)
            df_new = pl.read_csv(fname, columns= col_list)
            df_new =  df_new.with_columns(pl.col(col_list[5:]).cast(pl.Int64, strict=False))
            #df_select = df_new.select(col_list)
            df = pl.concat([df,df_new], rechunk=True)

        df.write_parquet(f'./data/{q}_{year}_selected.parquet')



def combining(year=2022):
    """combines the quarterly records of a year into a DataFrame and saves it as a parquet file.
    Args:
        year (int, optional): year from which records should be pulled. Defaults to 2022.
    """
    # read the quarterly records into a DataFrame
    df_q1 = pl.read_parquet(f"./data/Q1_{year}_selected.parquet")
    df_q2 = pl.read_parquet(f"./data/Q2_{year}_selected.parquet")
    df_q3 = pl.read_parquet(f"./data/Q3_{year}_selected.parquet")
    df_q4 = pl.read_parquet(f"./data/Q4_{year}_selected.parquet")
    #combining the quarterly records
    df_year_1 = df_q1.vstack(df_q2)
    df_year_2 = df_q3.vstack(df_q4)
    df_year = df_year_1.vstack(df_year_2)
    df_year.write_parquet(f'./data/{year}_data_selected.parquet')


def import_function(year=2022):
    """function to import data for a whole year and save it as a parquet file.

    Args:
        year (int, optional): year from which records should be imported. Defaults to 2022.
    """
    create_parquet(year=year)
    combining(year=year)

if __name__ == "__main__":
    import_function()