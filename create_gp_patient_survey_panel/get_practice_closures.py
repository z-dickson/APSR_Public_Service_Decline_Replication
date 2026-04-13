import pandas as pd 
import numpy as np
import polars as pl 



# merge with the practice data

def gp_closures():
    ## read in the GP practices data and rename the columns. 

    ## The data are available from https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data

    # epraccur.csv has no header row — read with header=None and assign names directly.
    gp_closures = pd.read_csv('../data/epraccur.csv', header=None)
    gp_closures.columns = ['organisation code', 'name', 'national grouping',
                        'high level health geography', 'health centre name', 'address line 1', 'address line 2',
                        'address line 3', 'address line 4', 'postcode',
                        'open date', 'close date', 'status code', 'organisation sub-type code',
                        'commissioner', 'join provider date', 'left provider date',
                        'contact telephone number', 'null', 'null', 'null', 'amended record indicator', 'null', 'provider purchaser',
                        'null', 'Prescribing setting', 'null']

    # filter the data to only include GP practices only 
    gp_closures = gp_closures.loc[gp_closures['Prescribing setting'] == 4]

    del gp_closures['null']
    gp_closures.columns = gp_closures.columns.str.replace(' ', '_').str.lower().str.strip()


    # function to fix the date columns
    # Note: row 0 (the shifted header row) is already filtered out by the Prescribing setting == 4
    # filter above, so there is no need to null it out here.
    def fix_dates(df, date_cols):
        df[date_cols] = df[date_cols].astype(float)
        df[date_cols] = df[date_cols].astype(str).str.replace('.0', '')
        return pd.to_datetime(df[date_cols], format='%Y%m%d', errors='coerce')

    gp_closures['close_date'] = fix_dates(gp_closures, 'close_date')
    gp_closures['open_date'] = fix_dates(gp_closures, 'open_date')

    # convert all object columns to string

    col_names = gp_closures.select_dtypes(include=['object', 'category']).columns
    for col in col_names:
        gp_closures[col] = gp_closures[col].astype(str)

    gp_closures = pl.DataFrame(gp_closures)
    # replace all string 'nan' with None 
    gp_closures = gp_closures.with_columns(pl.col(pl.String).replace("nan", None))



    # link postcodes to MSOA codes
    postcodes = pl.read_parquet('../data/postcodes_2023.parquet')
    gp_closures = gp_closures.join(postcodes, left_on='postcode',right_on='pcds',how='left')

    # Save closure coordinates CSV (used by fig2, figA1, figA2)
    (gp_closures.filter(pl.col("close_date").is_not_null())
     .with_columns(pl.col("close_date").dt.year().alias("close_year"))
     .select(["organisation_code", "name", "postcode", "close_date", "close_year", "lat", "long", "oslaua"])
     .write_csv("../data/gp_closures_coords.csv"))

    return gp_closures