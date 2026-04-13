
import polars as pl 
import pandas as pd 
import numpy as np


## function to read in data using sheet name as input

## sheets in data: Migration Flows, Short-Term Migration Inflows, Non-UK Born Population, Non-British Population, NINo Registrations, GP Registrations, Births to Non-UK Born Mothers

def data(sheet_name):
    x = pd.read_excel('../data/UK_GP_registrations_of_migrants_per_local_authority.xlsx', sheet_name=sheet_name)
    x.rename(columns={'Unnamed: 0':'area_code', 'Unnamed: 1': 'area_name'}, inplace=True)
    return x
    
    
    
## clean nino_registrations data 

def clean_nino_registrations():
    df = data('NINo Registrations')
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    start_year = 2011
    year_increment = 1
    columns_with_years = []
    for i, column in enumerate(df.columns[2:]):
        year = start_year + (i // 2) * year_increment
        new_column_name = f"{column}_{year}"
        columns_with_years.append(new_column_name)
    
    df.columns = df.columns[:2].tolist() + columns_with_years  
    
    # drop first two rows and bullshit rows at the end
    df = df.iloc[2:-10].reset_index(drop=True)  
    
    for col in df.columns[2:]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df.rename(columns={'Area Code': 'area_code','Area Name':'area_name'}, inplace=True)

    # remove the 'mid year X' from the column names to use as stubnames in wide_to_long
    df.columns.values[2:] = df.columns[2:].str.slice(8,).str.strip().str.replace("\n", " ")
    
    df=df.dropna()
    
    df = pd.wide_to_long(df, stubnames=["Population Estimate  (16 to 64)", "NINo Registrations"], 
                    sep = '_', i=['area_code', 'area_name'], j='year').reset_index()
    
    # add a year to the year column because the data is for mid year estimates
    df['year'] = df['year'].astype(int) + 1
    
    df=df.dropna()
    
    
    return df 






# clean Migration flows data

def clean_migration_flows():
    df = data('Migration Flows')
    df.columns = df.iloc[0]
    df = df[1:]
    cols = df.columns.transpose()
    row1 = df.iloc[0].transpose()
    df.columns = [str(a).replace('nan', '') + ' ' + str(b).replace('nan', '') for a, b in zip(row1, cols)]
    df = df.drop([1]).reset_index(drop=True)
    df.columns = df.columns.str.replace("\n", "").str.strip()
    
    # drop population estimates 
    df = df.drop(df.columns[2::5], axis=1)
    
    # add a year value to each column, starting with 2011 and incrementing by 1 for every 4 columns
    start_year = 2011
    year_increment = 1
    columns_with_years = []
    for i, column in enumerate(df.columns[2:]):
        year = start_year + (i // 4) * year_increment
        new_column_name = f"{column}_{year}"
        columns_with_years.append(new_column_name)
        
    df.columns = ['area_code','area_name'] + columns_with_years
    
    # rename outflow var(columns) to match inflow var(columns)
    df.columns.values[3::2] = 'Outflow ' + df.columns[2::2].str.replace('Inflow', '').str.strip()
    
    df.rename(columns={'Area Code_2011':'area_code', 'Area Name_2011':'Area Name'}, inplace=True)
    
    # drop NA and bullshit values 
    df = df.dropna(subset=['area_code']).reset_index(drop=True)[:-22]
    
    for col in df.columns[2:]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df.rename(columns={'Area Code': 'area_code','Area Name':'area_name'}, inplace=True)

    df = pd.wide_to_long(df, stubnames=['Inflow Long-Term International Migration', 'Outflow Long-Term International Migration','Inflow Internal Migration (within UK)', 'Outflow Internal Migration (within UK)'],
                    sep = '_', i=['area_code', 'area_name'], j='year').reset_index()
    
    # add a year to the year column because the data is for mid year estimates
    df['year'] = df['year'].astype(int) + 1
    return df
    
    







### clean Migrant GP Registrations data 

def clean_gp_registrations():
    df = data('GP Registrations')
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    start_year = 2011
    year_increment = 1
    columns_with_years = []
    for i, column in enumerate(df.columns[2:]):
        year = start_year + (i // 2) * year_increment
        new_column_name = f"{column}_{year}"
        columns_with_years.append(new_column_name)

    df.columns = df.columns[:2].tolist() + columns_with_years
    
    df.columns.values[2:] = df.columns[2:].str.slice(8, ).str.strip()
    
    for col in df.columns[2:]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df = df.dropna()
    
    df.rename(columns={'Area Code': 'area_code','Area Name':'area_name'}, inplace=True)
    
    df = pd.wide_to_long(df, stubnames=["Population Estimate", "ant GP Registrations"], 
                    sep = '_', i=['area_code', 'area_name'], j='year').reset_index()
    
    df = df.rename(columns={'Population Estimate': 'LAD_population_estimate', 'ant GP Registrations': 'migrant_gp_registrations'})
    
    # add a year to the year column because the data is for mid year estimates
    df['year'] = df['year'].astype(int) + 1
    
    return df 






def merge_immigration_covariates_with_USOC_data(bes):
    
    # check if the bes data is a polars dataframe
    if not isinstance(bes, pl.DataFrame):
        bes = pl.DataFrame(bes)
    
    gp_registrations = clean_gp_registrations()
    gp_registrations = pl.DataFrame(gp_registrations)

    nino_registrations = clean_nino_registrations()
    nino_registrations = pl.DataFrame(nino_registrations)

    migration_flows = clean_migration_flows()
    migration_flows = pl.DataFrame(migration_flows)
    
    # convert year in int64 
    bes = bes.with_columns(
        pl.col('year').cast(pl.Int64),
    )

    # merge BES data with migration data using local authority code and year 
    bes = bes.join(gp_registrations, left_on=['oslaua', 'year'], right_on = ['area_code', 'year'],  how='left')

    # merge BES data with migration data using local authority code and year
    bes = bes.join(migration_flows, left_on=['oslaua', 'year'], right_on = ['area_code', 'year'],  how='left', coalesce=True)
    
    # remove area_code and area_code_right columns
    bes = bes.drop(['area_name', 'area_name_right'])
    
    # merge BES data with NINO data using local authority code and year
    bes = bes.join(nino_registrations, left_on=['oslaua', 'year'], right_on = ['area_code', 'year'],  how='left', coalesce=True)
    
    # remove area_code and area_code_right columns
    bes = bes.drop(['area_name'])
    
    # make all columns lowercase and replace spaces with underscores   
    cols = bes.columns
    cols = [col.lower().replace(' ', '_').replace(")", '').replace("(", '').replace('-', '').replace('__', '_') for col in cols]
    bes.columns = cols 
    # rename columns in polars dataframe
    
    
    return bes 








def merge_immigration_covariates_with_BES_data(bes):
    
    # check if the bes data is a polars dataframe
    if not isinstance(bes, pl.DataFrame):
        bes = pl.DataFrame(bes)
    
    gp_registrations = clean_gp_registrations()
    gp_registrations = pl.DataFrame(gp_registrations)

    nino_registrations = clean_nino_registrations()
    nino_registrations = pl.DataFrame(nino_registrations)

    migration_flows = clean_migration_flows()
    migration_flows = pl.DataFrame(migration_flows)
    
    try: 
        bes = bes.with_columns(
            pl.col.starttime.cast(pl.Date).alias("date"),
            pl.col.date.dt.year().cast(pl.Int64).alias("year"),
        )
    
    except pl.ColumnNotFoundError:
        bes = bes.with_columns(
            pl.col.date.cast(pl.Date).alias("date"),
            pl.col.date.dt.year().cast(pl.Int64).alias("year"),
        )

    # merge BES data with migration data using local authority code and year 
    bes = bes.join(gp_registrations, left_on=['oslaua_code', 'year'], right_on = ['area_code', 'year'],  how='left')

    # merge BES data with migration data using local authority code and year
    bes = bes.join(migration_flows, left_on=['oslaua_code', 'year'], right_on = ['area_code', 'year'],  how='left', coalesce=True)

    # remove area_code and area_code_right columns
    bes = bes.drop(['area_name', 'area_name_right'])
    
    # merge BES data with NINO data using local authority code and year
    bes = bes.join(nino_registrations, left_on=['oslaua_code', 'year'], right_on = ['area_code', 'year'],  how='left', coalesce=True)
    
    # remove area_code and area_code_right columns
    bes = bes.drop(['area_name'])
    
    # make all columns lowercase and replace spaces with underscores   
    cols = bes.columns
    cols = [col.lower().replace(' ', '_').replace(")", '').replace("(", '').replace('-', '').replace('__', '_') for col in cols]
    bes.columns = cols 
    # rename columns in polars dataframe
    
    
    return bes 




def merge_immigration_covariates_with_GPPS_data(df):
    
    # check if the bes data is a polars dataframe
    if not isinstance(df, pl.DataFrame):
        df = pl.DataFrame(df)
    
    gp_registrations = clean_gp_registrations()
    gp_registrations = pl.DataFrame(gp_registrations)

    nino_registrations = clean_nino_registrations()
    nino_registrations = pl.DataFrame(nino_registrations)

    migration_flows = clean_migration_flows()
    migration_flows = pl.DataFrame(migration_flows)
    
    df = df.with_columns(
        pl.col('year').cast(pl.Int64),
    )

    # merge df data with migration data using local authority code and year 
    df = df.join(gp_registrations, left_on=['oslaua', 'year'], right_on = ['area_code', 'year'],  how='left')

    # merge df data with migration data using local authority code and year
    df = df.join(migration_flows, left_on=['oslaua', 'year'], right_on = ['area_code', 'year'],  how='left', coalesce=True)
    
    # remove area_code and area_code_right columns
    df = df.drop(['area_name', 'area_name_right'])
    
    # merge df data with NINO data using local authority code and year
    df = df.join(nino_registrations, left_on=['oslaua', 'year'], right_on = ['area_code', 'year'],  how='left', coalesce=True)
    
    # remove area_code and area_code_right columns
    df = df.drop(['area_name'])
    
    # make all columns lowercase and replace spaces with underscores   
    cols = df.columns
    cols = [col.lower().replace(' ', '_').replace(")", '').replace("(", '').replace('-', '').replace('__', '_') for col in cols]
    df.columns = cols
    # rename columns in polars dataframe
    
    
    return df 




# function to get the proportion of migrants in each local authority


def get_proportion_of_migrants(df):
    if not isinstance(df, pl.DataFrame):
        df = pl.DataFrame(df)
        
    df = df.with_columns(
        np.log((pl.col('migrant_gp_registrations') / pl.col('lad_population_estimate'))).alias('migrant_gp_registrations_proportion'),
        
        np.log((pl.col('nino_registrations') / pl.col('lad_population_estimate')).alias('nino_registrations_proportion')),
        
        np.log((pl.col('inflow_longterm_international_migration') / pl.col('lad_population_estimate'))).alias('inflow_longterm_international_migration_proportion'),
        
        np.log((pl.col('outflow_longterm_international_migration') / pl.col('lad_population_estimate'))).alias('outflow_longterm_international_migration_proportion'),
        
        np.log((pl.col('inflow_internal_migration_within_uk') / pl.col('lad_population_estimate'))).alias('inflow_internal_migration_within_uk_proportion'),
        
        np.log((pl.col('outflow_internal_migration_within_uk') / pl.col('lad_population_estimate'))).alias('outflow_internal_migration_within_uk_proportion')
    )
    return df


