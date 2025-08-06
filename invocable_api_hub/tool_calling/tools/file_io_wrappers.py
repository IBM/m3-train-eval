
import pandas as pd
import functools
import os
from uuid import uuid4


def update_docstring(docstring: str, input: bool = True, output: bool = True) -> str:
    lines = docstring.split('\n')
    nextline = False
    newlines = []
    for l in lines:
        if input and l.strip().startswith("data ("):
            l = "    data_source (str): The location of the data file in csv format. "
        if input and l.strip().startswith("data_1 ("):
            l = "    data_source_1 (str): The location of the first data file in csv format. "
        if input and l.strip().startswith("data_2 ("):
            l = "    data_source_2 (str): The location of the second data file in csv format. "
        if output and l.strip().startswith("Returns:"):
            nextline = True
            newlines.append(l)
            continue
        if nextline:
            l = "    str: The path to a csv file containing " + l.split(":", 1)[1].lower()
            nextline = False
        newlines.append(l)
    newstr = '\n'.join(newlines)
    return newstr

def load_csv_as_dataframe(func, tempfile_location: str):
    def wrapper(data_source: str, *args, **kwargs):
        file_path = os.path.join(tempfile_location, "temp_" + str(uuid4()) + ".csv")
        df = pd.read_csv(data_source, low_memory=False)
        dic = df.to_dict(orient='list')
        result = func(dic, *args, **kwargs)
        try:
            pd_result = pd.DataFrame(result)
            pd_result.to_csv(file_path, index=False)
            return file_path
        except:
            return result
    wrapper.__doc__ = update_docstring(func.__doc__, input=True, output=True)
    wrapper.__name__ = func.__name__
    return wrapper

def load_multiple_csvs_as_dataframes(func, tempfile_location: str):
    def wrapper(data_source_1: str, data_source_2: str, *args, **kwargs):
        file_path = os.path.join(tempfile_location, "temp_" + str(uuid4()) + ".csv")
        df1 = pd.read_csv(data_source_1, low_memory=False)
        df2 = pd.read_csv(data_source_2, low_memory=False)
        dic1 = df1.to_dict(orient='list')
        dic2 = df2.to_dict(orient='list')
        result = func(dic1, dic2, *args, **kwargs)
        try:
            pd_result = pd.DataFrame(result)
            pd_result.to_csv(file_path, index=False)
            return file_path
        except:
            return result
    wrapper.__doc__ = update_docstring(func.__doc__, input=True, output=True)
    wrapper.__name__ = func.__name__
    return wrapper

def save_as_csv(func, tempfile_location: str):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        file_path = os.path.join(tempfile_location, "temp_" + str(uuid4()) + ".csv")
        result = func(*args, **kwargs)
        try:
            pd_result = pd.DataFrame(result)
            pd_result.to_csv(file_path, index=False)
            return file_path
        except:
            return result
    wrapper.__doc__ = update_docstring(func.__doc__, input=False, output=True)
    wrapper.__name__ = func.__name__
    return wrapper


def load_from_csv(func):
    @functools.wraps(func)
    def wrapper(data_source: str, *args, **kwargs):
        df = pd.read_csv(data_source, low_memory=False)
        result = func(df.to_dict(orient='list'), *args, **kwargs)
        return result
    wrapper.__doc__ = update_docstring(func.__doc__, input=True, output=False)
    wrapper.__name__ = func.__name__
    return wrapper
