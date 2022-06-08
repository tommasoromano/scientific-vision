import pandas as pd
import numpy as np
import datetime
import pyspark.sql.dataframe
import pyspark.sql.session
import pyspark.sql.types
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import Imputer
import json
import missingno as msno
from sklearn.impute import SimpleImputer
from sklearn.impute import KNNImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
import smartbear

################################################################
#
#   VISUALIZATION
#
################################################################
#region UTILS

def missing_matrix(df:pyspark.sql.dataframe.DataFrame):
    '''
    Visualizing the locations of the missing data.
    
    https://github.com/ResidentMario/missingno

    The msno.matrix nullity matrix is a data-dense display 
    which lets you quickly visually pick out patterns in data completion.
    '''
    msno.matrix(df, labels=True)
    return

def missing_heatmap(df:pyspark.sql.dataframe.DataFrame):
    msno.heatmap(df)
    return

def missing_bar(df:pyspark.sql.dataframe.DataFrame):
    msno.bar(df)
    return

def missing_dendogram(df:pyspark.sql.dataframe.DataFrame):
    msno.dendrogram(df)
    return

#endregion
################################################################
#
#   DELETIONS
#
################################################################
#region DELETIONS

def null_columns(df:pyspark.sql.dataframe.DataFrame, 
    min=0.0, max=0.01)->list:
    '''
    returns the list of columns name that are NULL
    - range [min, max] = if percentage of missing values per column is in range, 
    the column is added to the NULL list
    - note: if range like [0.01, 1] you are getting NOT NULL cols
    '''
    assert min < 0.0 or min > 1.0
    assert max < 0.0 or max > 1.0
    assert min > max
    null_cols = []
    for c in df.columns:
        tmp = df.select(c)
        null_count = tmp.filter(F.col(c).isNotNull()).count()
        percent = null_count / tmp.count()
        if percent >= min and percent <= max:
            null_cols.append(c)
    return null_cols

def select_not_null_columns(df:pyspark.sql.dataframe.DataFrame, 
    not_null_columns:list=None,
    min=0.0, max=0.01)->pyspark.sql.dataframe.DataFrame:
    '''
    Returns a DataFrame without NULL columns
    - if not_null_columns is None, this will calculate not_null_columns
    using the difference between df.columns() and null_columns()
    '''
    if not_null_columns:
        res = df.select(not_null_columns)
    else:
        not_null_columns = null_columns(df, max, 1)
        res = df.select(not_null_columns)
    return res

#endregion
################################################################
#
#   IMPUTATIONS FOR NON TIME SERIES
#
################################################################
#region UTILS

def __fill_missing_with_imputer__(df:pyspark.sql.dataframe.DataFrame,
    columns, pdf:pd.DataFrame, imputer)->pyspark.sql.dataframe.DataFrame:
    if type(columns) is str:
        pdf[columns] = imputer.fit_transform(pdf[[columns]])
    elif type(columns) is list:
        for c in columns:
            pdf[c] = imputer.fit_transform(pdf[[c]])
    sb = smartbear.sb_spark()
    return sb.spark.createDataFrame(pdf, schema=df.schema)
    return

def fill_missing_with_constant(df:pyspark.sql.dataframe.DataFrame,
    columns, fill_value)->pyspark.sql.dataframe.DataFrame:
    '''
    - coloumns
    - fill_value

    see https://scikit-learn.org/stable/modules/generated/sklearn.impute.SimpleImputer.html
    '''

    pdf = (pd.DataFrame(df.toPandas()))
    imputer = SimpleImputer(strategy='constant', fill_value=fill_value)
    return __fill_missing_with_imputer__(df,columns,pdf,imputer)

def fill_missing_with_mean(df:pyspark.sql.dataframe.DataFrame,
    columns)->pyspark.sql.dataframe.DataFrame:
    '''
    - coloumns

    see https://scikit-learn.org/stable/modules/generated/sklearn.impute.SimpleImputer.html
    '''

    pdf = (pd.DataFrame(df.toPandas()))
    imputer = SimpleImputer(strategy='mean')
    return __fill_missing_with_imputer__(df,columns,pdf,imputer)

def fill_missing_with_median(df:pyspark.sql.dataframe.DataFrame,
    columns)->pyspark.sql.dataframe.DataFrame:
    '''
    - coloumns

    see https://scikit-learn.org/stable/modules/generated/sklearn.impute.SimpleImputer.html
    '''

    pdf = (pd.DataFrame(df.toPandas()))
    imputer = SimpleImputer(strategy='median')
    return __fill_missing_with_imputer__(df,columns,pdf,imputer)

def fill_missing_with_most_frequent(df:pyspark.sql.dataframe.DataFrame,
    columns)->pyspark.sql.dataframe.DataFrame:
    '''
    - coloumns

    see https://scikit-learn.org/stable/modules/generated/sklearn.impute.SimpleImputer.html
    '''

    pdf = (pd.DataFrame(df.toPandas()))
    imputer = SimpleImputer(strategy='most_frequent')
    return __fill_missing_with_imputer__(df,columns,pdf,imputer)

def fill_missing_with_KNN(df:pyspark.sql.dataframe.DataFrame,
    columns, n_neighbors=2, weights="uniform")->pyspark.sql.dataframe.DataFrame:
    '''
    - column: str like avg(valueQuantity_value) or list of columns
    - n_neighbors
    - weights

    see https://scikit-learn.org/stable/modules/generated/sklearn.impute.KNNImputer.html
    '''
    pdf = pd.DataFrame(df.toPandas())
    imputer = KNNImputer(n_neighbors=n_neighbors, weights=weights)
    return __fill_missing_with_imputer__(df,columns,pdf,imputer)

#endregion