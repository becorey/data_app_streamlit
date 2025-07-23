import os
import datetime

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
