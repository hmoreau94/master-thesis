# -*- coding: utf-8 -*-

"""
    Module containing all the functions that are considered as utility which is mainly 
    the functions that allow us to import the data in python from the orignal samples 
    but also some other tools that are used along the project (e.g. a progress bar)
"""
__author__ = "Hugo Moreau"
__email__ = "hugo.moreau@epfl.ch"
__status__ = "Prototype"

import os,re,sys
import pandas as pd
import copy
from datetime import timedelta

from scripts.preprocessing import *
from scripts.model_selection import *
from scripts.plot import *


### --------------------------------------------------------------------------------------------
### ----------------------------------------Diverse Tools---------------------------------------
### --------------------------------------------------------------------------------------------
def progress(count, total, suffix=''):
    """ 
    Shows the progress of a given action, using a progress bar
    
    Parameters
    -------------
    count: int
        the current count of done operations
    total: int
        the total number of operation to do
    suffix: string, optional
        a message printed after the progress bar
    """

    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '#' * filled_len + '-' * (bar_len - filled_len)


    sys.stdout.write('[%s] %s%s (%s/%s) %s\r' % (bar, percents, '%', count,total,suffix))
    if(count==total):
        sys.stdout.write('\n')
    sys.stdout.flush()

def get_longest_date_seq(dates,verbose = True):
    """ 
    Returns the longuest sequence (consecutive) of dates as a list of dates.
    
    Parameters
    -------------
    dates: numpy ndarray 
        dates where we wish to find the longuest sequence
    verbose: boolean, default True
        an be set to False if we do not wish to print the 
        details of the longuest sequence found

    Parameters
    -------------
    max_seq: list(dates)
        longuest sequence
    """
    days = sorted(list(pd.to_datetime(np.unique(dates))))
    assert(len(days) != 0), 'There are no dates'
    max_seq = []
    max_l = 0

    curr_seq = []
    curr_l = 0

    for d in days[1:]:
        if len(curr_seq)==0:
            curr_seq = [d]
            curr_l = 1
        elif(d == curr_seq[-1] + timedelta(days=1)):
            curr_seq.append(d)
            curr_l += 1
        else:
            if(curr_l > max_l):
                # we replace the max sequence only if it is longer than the max one
                max_seq = copy.deepcopy(curr_seq)
                max_l = curr_l

                curr_seq = []
                curr_l = 0
    if(verbose):
        print('The longuest sequence of dates has length {}:\n\tS: {}\n\tE: {}'.format(max_l,max_seq[0],max_seq[-1]))        
    return max_seq

### --------------------------------------------------------------------------------------------
### ----------------------------------------Data importation------------------------------------
### --------------------------------------------------------------------------------------------
def usable_data(date_string, data_location):
    """
    To get data that is directly usable for our machine learning, it will import the sample, 
    extract the ML usable data, convert labels to binary and finally encode the categorical features.

    Parameters
    -------------
    date_string: string
        to identify the data sample we wish to work with 
        e.g. '27_04' to work with 'sample_27_04.xlsx'
    data_location: string
        where the data sample can be found (e.g. "./Data")

    Returns
    -------------
    x: numpy ndarray
        inputs of the machine learning 
    y: numpy ndarray
        the labels of the input vectors to teach the Machine Learning algorithm
    dates: numpy ndarray 
        the dates of the input vector
    """
    data_path = data_location + '/sample_'+date_string+'.xlsx'
    backup_path = data_location+'/sample_'+date_string+'.pk'

    # We use the help function in order to obtain the dataframe in a correct format.
    extracted = import_sample(data_path,backup_path)
    dates = extracted['day_0'].values

    # Extract the raw data
    x_extracted, y_extracted = get_ml_data(extracted)

    # Perform binarization of labels and encoding of categorical values 
    y = convert_to_binary_labels(y_extracted)
    x_encoded = encode_categorical(x_extracted)

    # remove correlated features
    x_df = remove_features(x_encoded,verbose = True)
    x = x_df.values
    return x,y,dates


def excel_to_df(source_file_path, dump_file_path):
    """
    This function helps us read an excel file efficiently into a Pandas dataframe: 
    if a serialized version exists it will load it from there to avoid processing 
    it twice, if it isn't the case it will process it and serialize it.

    Parameters
    -------------
    source_file_path: string
        the path to the excel file we are interested in
    dump_file_path: string
        the path to file where the serialized version is 
        stored or where it should be stored 
    
    Returns
    -------------
    df: pandas DataFrame
        containing the data imported from the excel file

    """
    if(os.path.isfile(dump_file_path)):
        print('Retrieving from '+dump_file_path)
        df = pd.read_pickle(dump_file_path)
        return df
    else:
        if(os.path.isfile(source_file_path)):
            print('Reading '+source_file_path)
            df = pd.read_excel(source_file_path)
            print('Saving to '+dump_file_path)
            df.to_pickle(dump_file_path)
            return df
        else:
            print('The source file cannot be found : ' + source_file_path)


def import_sample(source_excel_path, dump_file_path, hw_models_2_id = None, delete_only_healthy_days = True):
    """
    Will import the sample of data from a source xlsx file, and will dump it to 
    increase future import performances. 
    Among others: it will
    * convert column names to lowercase
    * convert categorical features to categorical
    * convert the dates to correct format
    * translate the hardware model to a unique index
    * get rid of dates during which there is not a single CPE

    Parameters
    -------------
    source_excel_path: str
        the path to the excel file we are interested in
    dump_file_path: str
        the file that must be used to dump the results of the import, if the 
        file exists it will be used directly and no import operation will be performed
    hw_models_2_id: dict(str -> int), optional
        a dictionnary mapping the hardware model strings to indices. If set, it will be used to
        convert the model names to indices, otherwise there will be arbitrarily attributed indices 
        for each hardware model
    delete_only_healthy_days: boolean, default True
        can be set to true if we want to only import days that have both healthy and sick CPE.

    Returns
    -------------
    df: pandas Dataframe
        containing the dataframe with all the necessary transformations.
    """

    decompo = re.search(r"([\S]*)(sample[0-9_]*)([\S]*)",dump_file_path).groups()
    prefix_path = decompo[0] 
    name = decompo[1]
    extension = decompo[2]

    df = None
    dump_file_path = prefix_path+name + ('_sick_only' if delete_only_healthy_days else '_full') + '.pk'

    if(os.path.isfile(dump_file_path)):
        print('Retrieving from '+dump_file_path)
        df = pd.read_pickle(dump_file_path)
    else:
        if(os.path.isfile(source_excel_path)):
            # read it from the source
            print('Reading '+source_excel_path)
            df = pd.read_excel(source_excel_path)
            
            print('Performing some transformation')
            # we lower case the column names
            df.columns = map(str.lower, df.columns)
            original_cols = list(df.columns)

            # transforming dates to datetime and adding week day
            df['day_0'] = pd.to_datetime(df['day_0'],dayfirst = True)
            df['weekday'] = df['day_0'].apply(lambda x : x.weekday())

            # converting the hardware model to an ID
            translator = hw_models_2_id if hw_models_2_id else { 'CONNECT BOX CH7465LG COMPAL': 0,
                                                                 'UBEE EVM3206 (ED 3.0) - CPE': 1,
                                                                 'UBEE EVM3236 (ED 3.0) - CPE': 2,
                                                                 'WLAN MODEM EVW3226 - CPE': 3,
                                                                 'WLAN MODEM TC7200 - CPE': 4,
                                                                 'WLAN MODEM TC7200 V2 - CPE': 5,
                                                                 'WLAN MODEM TWG870 - CPE': 6}
            df['hardware_model'] = df['hardware_model'].map(translator)

            # transforming categories
            df['cmts'] = df['cmts'].astype('category')
            df['service_group'] = df['service_group'].astype('category')
            df['milestone_name'] = df['milestone_name'].astype('category')
            df['weekday'] = df['weekday'].astype('category')            

            # we reorganise the columns
            new_cols = ['weekday'] + original_cols
            df = df[new_cols]

            if(delete_only_healthy_days):
                # we only keep the days during which there are at least 1 sick CPE
                tmp =  df[['day_0']]
                tmp['sick'] = convert_to_binary_labels(df['milestone_name'])
                sick_per_day = tmp[['day_0','sick']].groupby(['day_0']).sum()
                no_entirely_healthy_day = sick_per_day[sick_per_day['sick'] > 0].index
                df = df[df['day_0'].isin(no_entirely_healthy_day)]

            # we serialize it

            print('Saving to ' + dump_file_path)
            df.to_pickle(dump_file_path)

        else:
            print('The source file cannot be found : '+source_excel_path)
            return df

    n_total,dimensions = df.shape
    n_sick = df['milestone_name'].count()
    n_healthy = n_total-n_sick
    print('The sample is composed of : {} vectors of dimension {}\n\tn_sick\t\t= {:>6}\n\tn_healthy\t= {:>6}'.format(n_total,dimensions,n_sick,n_healthy))
    return df

def get_ml_data(extracted_df,verbose = False):
    """
    Using the extracted dataframe, this function will return a dataframe that correspond to 
    the feature vectors and a numpy array corresponding to the classes.

    Parameters
    -------------
    extracted_df: pandas DataFrame 
        the dataframe containing all the raw data
    verbose: boolean, default False
        can be set to true if we wish to print informations about the number of feature columns

    Returns
    -------------
    inputs: pandas DataFrame
        the features we wish to use to train our algorithm
    targets: pandas Serie
        the labels of each sample in inputs
    """
    non_feature_cols = ['mac','day_0','cly_account_number',
                    'saa_account_number','cmts','service_group',
                    'seq_id','milestone_name']
    feature_cols = [x for x in list(extracted_df.columns) if x not in non_feature_cols]
    
    if(verbose):
        print('We are working with {} features'.format(len(feature_cols)))

    return extracted_df[feature_cols],extracted_df['milestone_name']

