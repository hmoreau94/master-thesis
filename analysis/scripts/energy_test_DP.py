# -*- coding: utf-8 -*-
"""
    Module containing all the functions that are used perform the analysis of the influence of 
    week day/weekend day on the vectors. It firts contains the naive approach and the energy test 
    that uses dynamic programming principles to avoid duplicated computation.
"""
__author__ = "Hugo Moreau"
__email__ = "hugo.moreau@epfl.ch"
__status__ = "Prototype"

import scipy, timeit
from scipy import stats
import numpy as np
import pandas as pd
import os
import sys

from scripts.utils import *
### --------------------------------------------------------------------------------------------
### ----------------------------------------Naive-----------------------------------------------
### --------------------------------------------------------------------------------------------
def get_ks_test_result(weekday_df,weekend_df,measurements,with_miss_mes=True):
    """
    Performs the Kolmogorov-Smirnov test on a list of measurement to detect whether the same measurement taken 
    from two population can be considered as being sampled from distinct distributions.

    Parameters
    -------------
    weekday_df : pandas Dataframe
        contains the first population
    weekend_df : pandas DataFrame
        contains the seconds population
    measurements: list(str)
        the list of columns that we wish to look at
    with_miss_mes: boolean, default True
        can be et to False to take out the Measurements that are prefixed by 'MISS' in the list of measurements
    
    Returns
    -------------
    results_df: pandas Datafram
        results of the test sorted by increasing p-values to show first the measurement that show 
        the most evidence of being sampled from different distributions

    """
    results = []
    considered_mes = measurements
    if(not with_miss_mes):
        considered_mes = [x for x in measurements if not('MISS' in x)]
    for mes in considered_mes:
        test = stats.ks_2samp(weekday_df[mes].values, weekend_df[mes].values)
        results.append({'measurement':mes,'statistic':test.statistic,'pvalue':test.pvalue})
    result_df = pd.DataFrame(results).set_index('measurement')[['statistic','pvalue']]
    return result_df.sort_values(by='pvalue',ascending=True)



### --------------------------------------------------------------------------------------------
### ----------------------------------------Energy test-----------------------------------------
### --------------------------------------------------------------------------------------------

def energy_two_sample_large_dataset(original_X,original_Y,n_bootstrap=99,alpha=1,print_exec_time=True,n_repeat=10,sample_size=10000,similar=False):
    """
    Similar to energy_two_sample_test but works on large data samples, so it will subsample 
    the population and run the experiment multiple times to estimate test results.
    
    Parameters
    -------------
    original_X: numpy ndarray
        d by n1 array matrix representing X_1, ...,X_n1 the random samples of the first population
    original_Y: numpy ndarray
        d by n2 array matrix representing Y_1, ...,Y_n2 the random samples of the second population
    n_bootstrap: int, default 99
        the number of boostrap ressampling
    alpha: int
        significance level (in 0-100)
    sample_size: int, default 10000
        the number of samples in each population (if it isn't available we will use the max available)
    similar: boolean, default False
        f we want to run the test by sampling two population 
        from only original_X (can be useful for reality checks)

    Returns
    -------------
    p_values: list(float)
        the p_value over each repetition 
    observed: list(float)
        the observed statistic
    limits: list(float)
        the limits that the statistic shouldn't reach
    """

    start = timeit.default_timer()
    assert (((n_bootstrap + 1)*(alpha/100)).is_integer()),"alpha isn't compatible with n_bootstrap"

    # Where we will store the outcome of each test
    p_values = []
    observed = []
    limits = []

    count = 0
    for i in range(n_repeat):
        if(count == 0):
            scripts.utils.progress(count+1,n_bootstrap*n_repeat)
        if(similar):
            assert(original_Y == None)
            sample_size = min(sample_size,original_X.shape[1]/2)
            sample = original_X[:,np.random.choice(original_X.shape[1],2*sample_size,replace=False)]
            x_samp = sample[:,:sample_size]
            y_samp = sample[:,sample_size:]
        else:
            # because we sample without replacement we take the maximum sample size up to the limit passed by argument
            sample_size = min(sample_size,min(original_X.shape[1],original_Y.shape[1]))
            x_samp = original_X[:,np.random.choice(original_X.shape[1],sample_size,replace=False)]
            y_samp = original_Y[:,np.random.choice(original_Y.shape[1],sample_size,replace=False)]

        # we will store the distances between two vectors in this matrix, it will not be n*n because 
        # it would then be storing twice the same info (as distance is symetric)
        d,n1 = x_samp.shape
        n2 = y_samp.shape[1]
        n = n1 + n2

        pooled = np.concatenate((x_samp,y_samp),axis=1)

        transposed = pooled.T
        distance_square_form = scipy.spatial.distance.pdist(transposed)
        distance = scipy.spatial.distance.squareform(distance_square_form)
         
        X_indices = np.arange(n1)
        Y_indices = np.arange(n1,n)
        
        E_distance_observed = compute_energy_statistic(X_indices,Y_indices,distance)
        E_distance_replicates = []
        
        for b in range(0,n_bootstrap):
            if(count != 0):
                progress(count+1,n_bootstrap*n_repeat)
            indices_Xb,indices_Yb = get_resampled_indices(n1, n2)
            E_distance = compute_energy_statistic(indices_Xb,indices_Yb,distance)
            E_distance_replicates.append(E_distance)
            count = count + 1
            
        percentile_lim = (100-alpha)
        lim = np.percentile(np.array(E_distance_replicates),percentile_lim)
        p_value = find_p_value(E_distance_replicates,E_distance_observed)

        p_values.append(p_value)
        observed.append(E_distance_observed)
        limits.append(lim)

    end = timeit.default_timer()

    mean_p = np.mean(p_values)
    std_p = np.std(p_values)

    if(mean_p < alpha/100):
        print("We reject the Null hypothesis (CL = {}%): p-value has mean={} and std={} ".format(100-alpha,mean_p,std_p))
    else:
        print("We cannot reject the Null hypothesis (CL = {}%): p-value has mean={} and std={} ".format(100-alpha,mean_p,std_p))
    if(print_exec_time):
        print("Execution time: {}s".format(round(end-start,4)))
    # and we return the replicates from the b ootstrap samples to allow further inspection
    return  p_values, observed, limits



def energy_two_sample_test(original_X,original_Y,n_bootstrap,alpha,distance_matrix_bkp=None,print_exec_time=True,use_bkp=True):
    """
    Performs the hypothesis testing: given two independent random 
    samples in R^d, it will test whether we can reject the null hypothesis
    (the two samples are sampled from the same distribution)
    at a significance level alpha. This method is based on "Testing for 
    equal distributions in high dimensions" Szekely and Rizzo 2004
    
    https://pdfs.semanticscholar.org/ad5e/91905a85d6f671c04a67779fd1377e86d199.pdf
    
    Rk: we must have (n_boostrap + 1)*alpha is an integer
    
    Parameters
    -------------
    original_X: numpy ndarray
        d by n1 array matrix representing X_1, ...,X_n1 the random samples of the first population
    original_Y: numpy ndarray
        d by n2 array matrix representing Y_1, ...,Y_n2 the random samples of the second population
    n_boostrap: int
        the number of boostrap ressampling 
    alpha: int
        significance level (in 0-100)
    distance_matrix_bkp: str, optional
        path to the backup of the distance matrix (if it already exists it will be used, otherwise it will be created)
    print_exec_time: boolean, default True
        can be set to true to also display the information regarding the running times
    use_bkp: boolean, default True
        can be set to True if we wish to use a backup of the distance matrix


    Returns
    -------------
    E_distance_replicates: list(float)
        replicates from the bootstrap samples to allow further inspection
    """
    start = timeit.default_timer()
    assert (alpha <= 100 and alpha >= 0),"Alpha should be expressed as a percentage in [0-100]"
    assert (((n_bootstrap + 1)*(alpha/100)).is_integer()),"alpha isn't compatible with n_bootstrap"

    # we will store the distances between two vectors in this matrix, it will not be n*n because 
    # it would then be storing twice the same info (as distance is symetric)
    d,n1 = original_X.shape
    n2 = original_Y.shape[1]
    n = n1 + n2

    pooled = np.concatenate((original_X,original_Y),axis=1)
    
    path = distance_matrix_bkp
    if(use_bkp and not path.endswith('.npy')):
        path = path+'.npy'

    # first we compute the distance matrix
    if(use_bkp and os.path.isfile(path)):
            print("Retrieving the distance matrix at: {}".format(path))
            distance_square_form = np.load(path)
            distance = scipy.spatial.distance.squareform(distance_square_form)
    else:
        print("Computing the distance matrix...")
        transposed = pooled.T
        distance_square_form = scipy.spatial.distance.pdist(transposed)
        distance = scipy.spatial.distance.squareform(distance_square_form)
        if(use_bkp):
            print("Saving the distance matrix at: {}".format(path))
            np.save(path,distance_square_form)
     
    X_indices = np.arange(n1)
    Y_indices = np.arange(n1,n)
    
    E_distance_observed = compute_energy_statistic(X_indices,Y_indices,distance)
    E_distance_replicates = []
    
    for b in range(0,n_bootstrap):
        progress(b+1,n_bootstrap)
        indices_Xb,indices_Yb = get_resampled_indices(n1, n2)
        E_distance = compute_energy_statistic(indices_Xb,indices_Yb,distance)
        E_distance_replicates.append(E_distance)
        

    percentile_lim = (100-alpha)
    lim = np.percentile(np.array(E_distance_replicates),percentile_lim)
    p_value = find_p_value(E_distance_replicates,E_distance_observed)
    end = timeit.default_timer()

    if(p_value < alpha/100):
        print("We reject the Null hypothesis (CL = {}%): p-value = {}\n\t observed = {} \t limit = {}".format(100-alpha,p_value,E_distance_observed,lim))
    else:
        print("We cannot reject the Null hypothesis (CL = {}%): p-value = {}\n\t observed = {} \t limit = {}".format(100-alpha,p_value,E_distance_observed,lim))
    if(print_exec_time):
        print("Execution time: {}s".format(round(end-start,4)))
    # and we return the replicates from the bootstrap samples to allow further inspection
    return E_distance_replicates

def compute_energy_statistic(indices_X, indices_Y, distance_matrix):
    """ 
    Computes the energy statistic for two group of independent random samples of random vectors

    Parameters
    -------------
    indices_X, indices_Y : numpy ndarray
        non-overlapping list of indices (referencing columns of pooled_vectors) of respective length n1 and n2
    pooled_vectors : pandas Dataframe
        stores the vectors of both groups
    distance_matrix : numpy ndarray
        n by n matrix storing distance between pairs of vectors from pooled_vectors

    Returns
    -------------
    E: float 
        the energy statistic
    """
    n1 = len(indices_X)
    n2 = len(indices_Y)

    term1 = 2/(n1*n2) * compute_double_sum_eucl(indices_X, indices_Y, distance_matrix)
    term2 = 1/(n1**2) * compute_double_sum_eucl(indices_X, indices_X, distance_matrix)
    term3 = 1/(n2**2) * compute_double_sum_eucl(indices_Y, indices_Y, distance_matrix)

    return (n1*n2)/(n1+n2)*(term1 - term2 - term3)

def compute_double_sum_eucl(indices_A, indices_B, distance_matrix):
    """
    Computes sum of euclidean norms between each possible pair of vectors in groups designated by indices_X and indices_Y

    Parameters
    -------------
    indices_A,indices_B: numpy ndarray
        non-overlapping arrays of size (n1,) and (n2,) representing index of columns from pooled vectors
    distance_matrix: numpy ndarray
        square ndarray (n,n) storing the euclidean distance between columns of pooled

    Returns
    -------------
    d: float
        the sum of distances
    """
    rows = indices_A[:,np.newaxis]
    cols = indices_B
    return distance_matrix[rows,cols].sum()

def get_resampled_indices(n1, n2):
    """
    Will compute the indices of the bootstrap samples from the pooled samples
    
    Parameters
    -------------
    n1,n2: int
        >0 integers giving the repective size of the first and second sample

    Returns
    -------------
    i1: the indices of the new first population
    i2: the indices of the new second population

    """

    n = n1 + n2 
    # get a permutation of indices
    shuffled = np.random.permutation(n)
    return shuffled[:n1],shuffled[n1:n]

def find_p_value(bootstrapped_energy_list, observed_value):
    """
    Computes the p-value obtained from the statistical test

    Parameters
    -------------
    bootstrapped_energy_list: list(float)
        a list of observed statistics computed from bootstrapped samples on the pool 
    observed_value: float
        the value observed on the original groups

    Returns
    -------------
    p-value: float
        the proportion of bootstrapped samples that are over the limit 
        (which gives us the p-value)
    """
    assert(len(bootstrapped_energy_list) != 0)
    count = 0
    for e in bootstrapped_energy_list:
        if(e>observed_value):
            count += 1
    return count/len(bootstrapped_energy_list)

