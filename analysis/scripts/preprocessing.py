# -*- coding: utf-8 -*-

"""
    Module containing all the functions that are used to preprocess the data (performing operation on 
    the data from its improt state to the point where it is usable for ML). It mainly contains: the real 
    preprocessing part and the correlation analysis that will get rid of features that are highly correlated
"""
__author__ = "Hugo Moreau"
__email__ = "hugo.moreau@epfl.ch"
__status__ = "Prototype"

import pandas as pd
import numpy as np
import sklearn

from scripts.utils import *
from scripts.model_selection import *
from scripts.plot import *

### --------------------------------------------------------------------------------------------
### ----------------------------------------Data preprocessing----------------------------------
### --------------------------------------------------------------------------------------------

def encode_categorical(feature_vec_df,selected_col=['hardware_model','weekday'], prefixes = ["model","wk"]):
    """
    Will encode selected columns of the feature vector dataframe using a one hot encoding

    Parameters
    -------------
    feature_vec_df: pandas Dataframe
        dataframe containing all the feature vectors
    selected_col: list(str), default ['hardware_model','weekday']
        the list of columns to transform
    prefixes: list(str), default ["model","wk"]
        the list of prefixes to prepend to each dummy col created for each selected col

    Returns
    -------------
    df_with_dummy: pandas Dataframe
        where the selected columns have been converted to dummy variables (one hot encoding)
    """
    return pd.get_dummies(feature_vec_df, columns = selected_col , prefix = prefixes)

def convert_to_binary_labels(y):
    """
    Converts the labels into binary. (It assumes that everything in y set to None belongs to class 0 and the rest to class 1)

    Parameters
    -------------
    y: pandas Serie
        the current label where only 'sick' CPEs have non-null labels

    Returns
    -------------
    y_binary: numpy ndarray
        where the labels have been replaced to binary
    """
    return y.isnull().map(lambda x: 0 if x else 1).values   

def impute_missing(feature_vec_df,method = 'zero'):
    """
    Will return a dataframe with no missing values.

    Parameters
    -------------
    feature_vec_df: pandas DataFrame 
        dataframe containing all the feature vectors
    method: str, default 'zero'
        the method to use to impute (from 'zero','mean','median')

    Returns
    -------------
    df: pandas Dataframe
        where the missing values have been imputed
    """
    assert(method in ['zero','mean','median']), 'The chosen method to impute is not valid'

    if(method == 'zero'):
        return feature_vec_df.fillna(0)
    elif(method in ['mean','median']):
        imputer = sklearn.preprocessing.Imputer(strategy=method)
        values_no_missing = imputer.fit_transform(feature_vec_df.values)
        return pd.DataFrame(values_no_missing, columns = feature_vec_df.columns)

def remove_features(x,verbose = True):
    """
    In order to remove the features spotted in the initial analysis as duplicated 
    or highly correlated with another one.

    Parameters
    -------------
    x: pandas DataFrame
        the input dataframe on which we wish to delete the features 
        that have been identified as highly correlated
    verbose: boolean, default True
        can be set to false if we do not want to print the proportion of features deleted

    Returns
    -------------
    df: pandas Dataframe
        where features have been dropped
    """
    suffixes_non_miss = ['','_6h','_12h','_18h','_1d','_2d','_3d','_4d','_5d']
    suffixes_miss = ['','_6h','_12h','_18h','_24h','_2d','_3d','_4d','_5d']

    to_drop = ['cmts_ms_utilization_up' + s for s in suffixes_non_miss] + \
                        ['miss_pct_traffic_sdmh_up' + s for s in suffixes_miss] + \
                        ['miss_rx_dn' + s for s in suffixes_miss] + \
                        ['miss_tx_up' + s for s in suffixes_miss] + \
                        ['miss_snr_up' + s for s in suffixes_miss]
    to_drop = to_drop + ['miss_cer_dn_1d','miss_cer_up_1d','miss_pct_traffic_dmh_up_1d',
        'miss_pct_traffic_sdmh_up_1d','miss_rx_dn_1d','miss_rx_up_1d','miss_snr_dn_1d',
        'miss_snr_up_1d','miss_tx_up_1d']
    if(verbose):
        deleted = len(to_drop)
        p = 100*deleted/len(x.columns)
        print('Deleting {} features ({:.3f}%).'.format(deleted,p))
    return  x.drop(labels=to_drop,axis=1)

def get_balanced_classes(x,y_binary):
    """
    In order to balance the dataset. Will return a randomly shuffled subsample 
    of the dataset that subsamples the positive class (healthy)

    Parameters
    -------------
    x: numpy ndarray
        the input data
    y_binary: numpy ndarray
        binary targets

    Returns
    -------------
    x_sampled: ndarray
        shuffled subsample of the inputs  
    y_sampled: ndarray
        shuffled subsample of the targets

    """
    vec_healthy = x[y_binary == 0]
    vec_sick = x[y_binary == 1]
    n_sick = len(vec_sick)

    # we select as many random indices from vec_healthy as we have routers that are sick
    sampled_indices = np.random.choice(len(vec_healthy), size=n_sick)

    sample_healthy = vec_healthy[sampled_indices]
    x_s = np.vstack((sample_healthy,vec_sick))
    y_s = np.vstack((np.zeros(n_sick).reshape(-1,1),np.ones(n_sick).reshape(-1,1)))
    shuffled_indices = np.random.permutation(len(x_s))
    return x_s[shuffled_indices],y_s[shuffled_indices]

### --------------------------------------------------------------------------------------------
### ----------------------------------------Correlation Analysis--------------------------------
### --------------------------------------------------------------------------------------------
def nearZeroVar(feature_vec_df,freqCut=99,ratioCut=95/5,uniqueCut=10):
    """
    Diagnoses features that have one unique value (i.e. are zero variance predictors) 
    or predictors that are have either of the following characteristics: 
        * very few unique values relative to the number of samples 
        * the ratio of the frequency of the most common value to the frequency of the second most common value is large. 

    Parameters
    -------------
    feature_vec_df: pandas DataFrame
        dataframe containing all the feature vectors
    freqCut: int, default 99
        freq of the most common value as a percentage of the number of samples
    ratioCut: float, default 95/5
        ratio of the most common value freq over the second most common over which we cut
    unqiueCut:  int, default 10
        ratio of unique values over the total number of all values

    Returns
    -------------
    to_Return: list(str)
        the list of columns in feature_vec_df that should be investigated
    """
    toReturn = []
    for col in feature_vec_df.columns:
        counts = feature_vec_df[col].value_counts()
        total = counts.sum()
        freqs = counts.apply(lambda x: 100*x/total)
        most_common = freqs.iloc[0]
        sec_most_common = freqs.iloc[1]
        
        r = most_common/sec_most_common
        unique_perc = 100*len(counts)/total

        if (most_common >= freqCut or (r >= ratioCut and unique_perc <= uniqueCut)):
            toReturn.append(col)

    return toReturn

def are_identical(df,col1,col2):
    """
    Given two columns in a pandas dataframe it will return whether the two columns are exactly identicals or not.
    
    Parameters
    -------------
    df: pandas DataFrame
        contains the data of the columns we wish to invetsigate
    col1: str
        name of the first column
    col2: str
        name of the second column

    Returns
    -------------
    identical: boolean
        true if all values are identical between both columns
    """
    t = df[[col1,col2]]
    return len(t[t[col1] != t[col2]]) == 0 

def add_pair_to_dict(dic,pair):
    """
    will add to dic a pair of strings such that if one of the element of the pair 
    is in the dic keys the other will be added to the list stored in the dict. 
    Otherwise it will create a list corresponding to the first element of 
    the pair and add the second element to this list
    
    Parameters
    -------------
    dic: dic(str -> {str})
        a dic where keys are strings and values are sets of strings
    -pair: (str,str)

    Returns
    -------------
    dic: dict(str -> {str})
        the newly updated dict
    """
    key = None
    value = None
    if(pair[0] in dic):
        key = pair[0]
        value = pair[1]
    elif(pair[1] in dic):
        key = pair[1]
        value = pair[0]
    
    if(not key):
        key = pair[0]
        value = pair[1]
        dic[key] = set()
    
    dic[key].update([value])
    return dic
        
def find_correlation(data, threshold=0.9):
    """
    Given a numeric pd.DataFrame, this will find highly correlated features,
    and return a list of features to remove.
    
    Parameters
    -------------
    data : pandas DataFrame
    threshold : float, default 0.9
        correlation threshold, will remove one of pairs of features with a
        correlation greater than this value
    
    Returns
    -------------
    select_flat: list(str) 
        list of column names to be removed
    """
    corr_mat = data.corr().apply(lambda x:abs(x))
    # we keep only the upper triangle
    corr_mat = corr_mat.where(np.triu(np.ones(corr_mat.shape), k=1).astype(np.bool))
    correlated_lists = {}
    identical_lists = {}
    result = []
    for col in corr_mat:
        col_corr_count = 0
        # for each col we look at other cols that are highly correlated
        perfect_corr = corr_mat[col][corr_mat[col] > threshold].index.tolist()
        
        for x in perfect_corr:
            # we handle a special case that must be due to wrong data collection
            if( (col in x or x in col) and are_identical(data,col,x)):
                identical_lists = add_pair_to_dict(identical_lists,(col,x))
            else:
                correlated_lists = add_pair_to_dict(correlated_lists,(col,x))
    return correlated_lists,identical_lists

def compare_candidate_identical(principal,secondaries,suffixes,data):
    """
    Helpers to look at joint distribution of two variables. It will plot principal against each of 
    the variables in secondaries for each time aggregate denoted by suffixes.

    Parameters
    -------------
    principal: str
        the name of the main variable to which we wish to compare others
    secondaries: list(str)
        a list of names of the other variables
    suffixes: list(str)
        the suffix to be appended to the variable names to get the different time aggregates
    data: pandas DataFrame 
        into which resides the data (where all the previous column are present)
    """
    clique = {}
    for s in suffixes:
        clique[principal+s] = set([x + s for x in secondaries])

    for key in clique:
        look_at_joint_dist(data,key,clique[key])