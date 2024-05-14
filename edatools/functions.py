import datetime as dt
import polars as pl

def dtvars(df_input, dtcols, dtlabels):
    """Creates multiple time-related columns out of a datetime column using polars"""
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

def cleaning_record(df, step_label, starting_rows='NA', clean_df='NA'):
    """Records the number of removed and remaining rows for a given data cleaning step"""
    if not isinstance(clean_df, pl.DataFrame):
        row_count = starting_rows-df.shape[0]
        clean_df = pl.DataFrame(data={'Cleaning Step': step_label,'Rows Removed':row_count,'Rows Remaining':df.shape[0]})
    else:
        row_count = clean_df[clean_df.shape[0]-1,2]-df.shape[0]
        new_row = pl.DataFrame({'Cleaning Step':step_label,'Rows Removed':row_count,'Rows Remaining':df.shape[0]})
        clean_df = pl.concat([clean_df,new_row])
    return clean_df

def weekday_names(df_input, wdcols):
    """Converts numerical weekdays into abbreviated weekday names"""
    dtype = pl.Enum(["Sun", "Mon", "Tue","Wed","Thu","Fri","Sat"])
    for i in range(len(wdcols)):
        df_output = df_input.with_columns(
            pl.col(wdcols[i]).replace({1:'Sun',2:'Mon',3:'Tue',4:'Wed',5:'Thu',6:'Fri',7:'Sat'})
        )
        df_output = df_output.with_columns(pl.col(wdcols[i]).cast(dtype=dtype))
    return df_output

def missing_data(df_input):
    df_missing = pl.DataFrame({'columns':df_input.columns,
                               'dtypes':df_input.dtypes,
                               'null_values':df_input.null_count().row(0)})
    return df_missing


