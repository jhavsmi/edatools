import datetime as dt
import polars as pl

def pl_dtvars(df_input, dtcols, dtlabels):
    '''
    Creates multiple time-related columns out of a datetime column using polars
    
    Arguments:
        df_input:
        dtcols:
        dtlabels:

    '''
    for i in range(len(dtcols)):
        df_input = df_input.with_columns(
        pl.col(dtcols[i]).dt.year().alias(dtlabels[i]+'_year'),
        pl.col(dtcols[i]).dt.month().alias(dtlabels[i]+'_month'),
        pl.col(dtcols[i]).dt.day().alias(dtlabels[i]+'_day'),
        pl.col(dtcols[i]).dt.week().alias(dtlabels[i]+'_week'),
        pl.col(dtcols[i]).dt.weekday().alias(dtlabels[i]+'_weekday'),
        pl.col(dtcols[i]).dt.hour().alias(dtlabels[i]+'_hour'),
        pl.col(dtcols[i]).dt.time().alias(dtlabels[i]+'_time')
    )
    return df_input

def pl_outlier_imputer(df, column_list, iqr_factor):
    '''
    Impute upper-limit values in specified columns based on their interquartile range.

    Arguments:
        column_list: A list of columns to iterate over
        iqr_factor: A number representing x in the formula:
                    Q3 + (x * IQR). Used to determine maximum threshold,
                    beyond which a point is considered an outlier.

    The IQR is computed for each column in column_list and values exceeding
    the upper threshold for each column are imputed with the upper threshold value.
    '''

    for col in column_list:
        # Reassign minimum to zero
        df = df.with_columns(pl.when(pl.col(col) < 0 )
               .then(0)
               .otherwise(pl.col(col)).alias(col))

        # Calculate upper threshold
        q1 = df.select(pl.col(col).quantile(0.25)).item()
        q3 = df.select(pl.col(col).quantile(0.75)).item()
        iqr = q3 - q1
        upper_threshold = q3 + (iqr_factor * iqr)
        print(col)
        print('q3:', q3)
        print('upper_threshold:', upper_threshold ,'\n')

        # Reassign values > threshold to threshold
        df = df.with_columns(pl.when(pl.col(col) > upper_threshold)
               .then(upper_threshold)
               .otherwise(pl.col(col)).alias(col))
        if 'desc_df' in locals():
            desc_df = desc_df.join(df[col].describe(),on='statistic')
        else: 
            desc_df = df[col].describe()
    
    desc_df.columns = ['statistic'] + column_list
    
    print(desc_df)
    return df      

        

def pl_cleaning_record(df, step_label, starting_rows='NA', clean_df='NA'):
    '''
    Records the number of removed and remaining rows for a given data cleaning step
    
    Arguments:
        df:
        step_label:
        starting_rows:
        clean_df:

    '''
    if not isinstance(clean_df, pl.DataFrame):
        row_count = starting_rows-df.shape[0]
        clean_df = pl.DataFrame(data={'Cleaning Step': step_label,'Rows Removed':row_count,'Rows Remaining':df.shape[0]})
    else:
        row_count = clean_df[clean_df.shape[0]-1,2]-df.shape[0]
        new_row = pl.DataFrame({'Cleaning Step':step_label,'Rows Removed':row_count,'Rows Remaining':df.shape[0]})
        clean_df = pl.concat([clean_df,new_row])
    return clean_df

def pl_weekday_names(df_input, wdcols):
    '''
    Converts numerical weekdays into abbreviated weekday names
    
    Arguments:
        df_input:
        wdcols:
        
    '''
    dtype = pl.Enum(["Sun", "Mon", "Tue","Wed","Thu","Fri","Sat"])
    for i in range(len(wdcols)):
        df_output = df_input.with_columns(
            pl.col(wdcols[i]).replace({1:'Sun',2:'Mon',3:'Tue',4:'Wed',5:'Thu',6:'Fri',7:'Sat'})
        )
        df_output = df_output.with_columns(pl.col(wdcols[i]).cast(dtype=dtype))
    return df_output

def pl_dtypes(df_input):
    '''
    Computes missing values and data type for each column in a polars dataframe.

    Arguments:
        df_input

    '''
    df_missing = pl.DataFrame({'columns':df_input.columns,
                               'dtypes':df_input.dtypes,
                               'null_values':df_input.null_count().row(0)})
    return df_missing

