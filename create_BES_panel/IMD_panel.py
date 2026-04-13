
import pandas as pd 
import numpy as np 
import polars as pl 



def imd(year):
    df = pl.read_parquet(f'../data/iod_{year}.parquet')
    return df 

def clean_2015(df, metric = 'Score'): 
    df = df.with_columns(
        pl.col('FeatureCode').alias('lsoa11'),
        pl.col('a. Index of Multiple Deprivation (IMD)').alias(f'IMD_{metric}'),
        pl.col('b. Income Deprivation Domain').alias(f'Income_Deprivation_{metric}'),
        pl.col('c. Employment Deprivation Domain').alias(f'Employment_Deprivation_{metric}'),
        pl.col('d. Education, Skills and Training Domain').alias(f'Education_Deprivation_{metric}'),
        pl.col('e. Health Deprivation and Disability Domain').alias(f'Health_Deprivation_{metric}'),
        pl.col('f. Crime Domain').alias(f'Crime_{metric}'),
        pl.col('g. Barriers to Housing and Services Domain').alias(f'Barriers_{metric}'),
        pl.col('h. Living Environment Deprivation Domain').alias(f'Living_Environment_{metric}'),
        pl.col('i. Income Deprivation Affecting Children Index (IDACI)').alias(f'IDACI_{metric}'),
        pl.col('j. Income Deprivation Affecting Older People Index (IDAOPI)').alias(f'IDAOPI_{metric}'),
    ).filter(
        pl.col('Measurement') == metric
    )
    #create year col 
    df = df.with_columns(
        year = 2015
        )
    return df 


def clean_2010(df, metric = 'Score'):
    
    df = df.with_columns(
        pl.col('LSOA CODE').alias('lsoa11'),
        pl.col('IMD SCORE').alias(f'IMD_{metric}'),
        pl.col('INCOME SCORE').alias(f'Income_Deprivation_{metric}'),
        pl.col('EMPLOYMENT SCORE').alias(f'Employment_Deprivation_{metric}'),
        pl.col('EDUCATION SKILLS AND TRAINING SCORE').alias(f'Education_Deprivation_{metric}'),
        pl.col('BARRIERS TO HOUSING AND SERVICES SCORE').alias(f'Barriers_{metric}'),
        pl.col('CRIME AND DISORDER SCORE').alias(f'Crime_{metric}'),
        pl.col('LIVING ENVIRONMENT SCORE').alias(f'Living_Environment_{metric}'),
        pl.col('IDACI score').alias(f'IDACI_{metric}'),
        pl.col('IDAOPI score').alias(f'IDAOPI_{metric}'),
    )
    #create year col 
    df = df.with_columns(
        year = 2010
        )
    return df 


def clean_2019(df, metric = 'Score'):
    df = df.with_columns(
        pl.col('LSOA code (2011)').alias('lsoa11'),
        pl.col('Index of Multiple Deprivation (IMD) Score').alias(f'IMD_{metric}'),
        pl.col('Income Score (rate)').alias(f'Income_Deprivation_{metric}'),
        pl.col('Employment Score (rate)').alias(f'Employment_Deprivation_{metric}'),
        pl.col('Education, Skills and Training Score').alias(f'Education_Deprivation_{metric}'),
        pl.col('Health Deprivation and Disability Score').alias(f'Health_Deprivation_{metric}'),
        pl.col('Crime Score').alias(f'Crime_{metric}'),
        pl.col('Barriers to Housing and Services Score').alias(f'Barriers_{metric}'),
        pl.col('Living Environment Score').alias(f'Living_Environment_{metric}'),
        pl.col('Income Deprivation Affecting Children Index (IDACI) Score (rate)').alias(f'IDACI_{metric}'),
        pl.col('Income Deprivation Affecting Older People (IDAOPI) Score (rate)').alias(f'IDAOPI_{metric}'),
    )
    #create year col 
    df = df.with_columns(
        year = 2019
        )
    return df



def combine_imd():
    
    a = clean_2010(imd(2010))
    b = clean_2015(imd(2015))
    c = clean_2019(imd(2019))
    
    
    df = pl.concat([a, b, c], how="align").select([
            'lsoa11',
            'year',
            'IMD_Score',
            'Income_Deprivation_Score',
            'Employment_Deprivation_Score',
            'Education_Deprivation_Score',
            'Health_Deprivation_Score',
            'Crime_Score',
            'Barriers_Score',
            'Living_Environment_Score',
            'IDACI_Score',
            'IDAOPI_Score',
        ])
    
    return df



# merge with geo data 

def merge_geo():
    geo = pl.read_parquet('../data/postcodes_2023.parquet')
    # get unique lsoa11
    geo = geo.unique('lsoa11')
    df = combine_imd()
    df = df.join(geo, on='lsoa11', how='left')
    df = df.group_by(
        ['msoa21', 'year']).agg(
            pl.col('IMD_Score').mean().alias('IMD_Score'),
            pl.col('Income_Deprivation_Score').mean().alias('Income_Deprivation_Score'),
            pl.col('Employment_Deprivation_Score').mean().alias('Employment_Deprivation_Score'),
            pl.col('Education_Deprivation_Score').mean().alias('Education_Deprivation_Score'),
            pl.col('Health_Deprivation_Score').mean().alias('Health_Deprivation_Score'),
            pl.col('Crime_Score').mean().alias('Crime_Score'),
            pl.col('Barriers_Score').mean().alias('Barriers_Score'),
            pl.col('Living_Environment_Score').mean().alias('Living_Environment_Score'),
            pl.col('IDACI_Score').mean().alias('IDACI_Score'),
            pl.col('IDAOPI_Score').mean().alias('IDAOPI_Score'),
        )
    
    
    return df



# function to interpolate missing values from teh IMD data

def interpolate_imd():
    df = merge_geo()
    df = df.sort('year')
    bp =[]
    years = np.arange(2009, 2025)
    msoas = df['msoa21'].unique()
    for year in years:
        for msoa in msoas:
            bp.append({'year': year, 'msoa21': msoa})
            
    bp = pl.DataFrame(bp)
    bp = bp.with_columns(
        pl.col('year').cast(pl.Int32),
        pl.col('msoa21').cast(pl.String),
    )
    bp = bp.join(df, on=['year', 'msoa21'], how='left')
    bp_pd = bp.to_pandas().sort_values(['msoa21', 'year'])
    imd_cols = [c for c in bp_pd.columns if c not in ('msoa21', 'year')]
    for col in imd_cols:
        bp_pd[col] = bp_pd.groupby('msoa21')[col].transform(
            lambda x: x.interpolate(method='linear')
        )
    bp = pl.DataFrame(bp_pd)
    return bp


def merge_with_bes(bes): 
    df = interpolate_imd()

    bes = bes.with_columns(
        year = bes['year'].cast(pl.Int32)
    )
    bes = bes.join(df, left_on = ['msoa21cd', 'year'], right_on=['msoa21', 'year'], how='left')
    
    return bes



def merge_with_usoc(bes): 
    df = interpolate_imd()

    bes = bes.with_columns(
        year = bes['year'].cast(pl.Int32)
    )
    bes = bes.join(df, left_on = ['msoa21', 'year'], right_on=['msoa21', 'year'], how='left')
    
    return bes




def merge_with_GPPS(df): 
    imd = interpolate_imd()
    geo = pl.read_parquet('../data/postcodes_2023.parquet')
    # get unique lsoa11
    geo = geo.unique('oslaua')
    
    # merge with imd 
    imd = imd.join(geo, on='msoa21', how='left')
    
    imd = imd.group_by(['oslaua', 'year']).agg(
        pl.col('IMD_Score').mean().alias('IMD_Score'),
        pl.col('Income_Deprivation_Score').mean().alias('Income_Deprivation_Score'),
        pl.col('Employment_Deprivation_Score').mean().alias('Employment_Deprivation_Score'),
        pl.col('Education_Deprivation_Score').mean().alias('Education_Deprivation_Score'),
        pl.col('Health_Deprivation_Score').mean().alias('Health_Deprivation_Score'),
        pl.col('Crime_Score').mean().alias('Crime_Score'),
        pl.col('Barriers_Score').mean().alias('Barriers_Score'),
        pl.col('Living_Environment_Score').mean().alias('Living_Environment_Score'),
        pl.col('IDACI_Score').mean().alias('IDACI_Score'),
        pl.col('IDAOPI_Score').mean().alias('IDAOPI_Score'),
    )
    
    df = df.with_columns(
        year = df['year'].cast(pl.Int32)
    )
    df = df.join(imd, left_on = ['oslaua', 'year'], right_on=['oslaua', 'year'], how='left')
    
    return df