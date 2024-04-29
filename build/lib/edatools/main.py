import datetime as dt
import pandas as pd

def dtvars(dfinput, dtcols, dtlabels):
    """This function creates multiple time-related columns out of a datetime column"""
    for i in range(len(dtcols)):
        dfinput[dtcols[i]] = pd.to_datetime(dfinput[dtcols[i]])
        dfinput[dtlabels[i]+'_year'] = dfinput[dtcols[i]].dt.year
        dfinput[dtlabels[i]+'_quarter'] = dfinput[dtcols[i]].dt.quarter
        dfinput[dtlabels[i]+'_month'] = dfinput[dtcols[i]].dt.strftime("%b")
        dfinput[dtlabels[i]+'_week'] = dfinput[dtcols[i]].dt.isocalendar().week
        dfinput[dtlabels[i]+'_day'] = dfinput[dtcols[i]].dt.day
        dfinput[dtlabels[i]+'_weekday'] = dfinput[dtcols[i]].dt.strftime("%a")
        dfinput[dtlabels[i]+'_hour'] = dfinput[dtcols[i]].dt.hour
        dfinput[dtlabels[i]+'_time'] = dfinput[dtcols[i]].dt.time
    return dfinput
        
