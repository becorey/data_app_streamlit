import os
import datetime
from zoneinfo import ZoneInfo
from scipy import stats
import numpy as np

def split_path(path):
    """
    split a fullpath into dir, base, ext
    ext will include the . i.e. ext = '.json'
    """
    dir = os.path.dirname(path)
    base, ext = os.path.splitext(os.path.basename(path))

    return dir, base, ext

def seconds_to_string(seconds):
    if seconds < 60:
        return str(int(seconds)) + 's'
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return str(int(minutes)) + 'm ' + str(int(seconds)) + 's'
    hours, minutes = divmod(minutes, 60)
    return str(int(hours)) + 'h ' + str(int(minutes)) + 'm ' + str(int(seconds)) + 's'


def timestamp_to_str(timestamp, timezone):
    try:
        utc_time = datetime.datetime.fromtimestamp(timestamp)
    except IndexError:
        return 'date_error'
    local_time = utc_time.astimezone(ZoneInfo(timezone))
    return local_time.strftime('%Y-%m-%d %I:%M:%S %p')


def remove_outliers(df, cols, zscore_threshold = 3):
    return df[(np.abs(stats.zscore(df[cols])) < zscore_threshold).all(axis=1)]
