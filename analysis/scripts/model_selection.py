# -*- coding: utf-8 -*-

"""
    Module containing all the functions that are used to chose the model we wish to use and that are not generating plots 
    (to be opposed to plots.py)
"""
__author__ = "Hugo Moreau"
__email__ = "hugo.moreau@epfl.ch"
__status__ = "Prototype"

import numpy as np
import sklearn
import itertools

from scripts.preprocessing import *
from scripts.model_selection import *
from scripts.plot import *

def get_cross_validated_metrics(clf,top_ratio,x,y,dates=None,dates_fold=False,cv=5):
    """
    Computes cross validated classification metrics on the top_ratio prediction

    Parameters
    -------------
    clf : sklearn pipeline or model on which we can call fit()
        the classifier
    top_ratio : float
        the ratio of top prediction we want to consider
    x: numpy ndarray 
        contains the input data
    y: numpy ndarray
        contians the target data
    dates: pandas serie, optionnal
        contains the day_0 date of the input vectors
    dates_fold: boolean, default False
        can be set to True if we wish to use a split with distinct dates in each fold
    cv: int, default 5
        number of folds to use to cross validate

    Returns
    -------------
    metrics: numpy array
        cross validated Precision, Recall and F1 score.
    """
    if(dates_fold):
        splits = distinct_date_split(x,y,dates,k=cv)
    else:
        kf = sklearn.model_selection.KFold(n_splits=cv)
        splits = kf.split(x)
    metrics = np.zeros((cv,3))
    for i,indices in enumerate(splits):
        train_index,test_index = indices
        x_train, x_test,y_train, y_test = x[train_index], x[test_index], y[train_index], y[test_index]
        
        clf.fit(x_train,y_train)
        y_sick_scores = clf.predict_proba(x_test)[:,1]
        m = get_metrics(y_sick_scores,y_test,top_ratio) 
        metrics[i,:] = m
    return np.mean(metrics,axis = 0)

def get_metrics(y_sick_scores,y_test,top_ratio):
    """
    Returns a string synthesizing the classification performance for the top prediction.
    That is we order the predictions based on the probability that the sample belongs to the 'sick'(=1) class
    and then look only at this subset to compute our metrics.
    
    Parameters
    -------------
    y_sick_scores: numpy ndarray
        array of probabilities of belonging to class 1
    y_test:  numpy ndarray
        the true class of each sample
    top_ratio: float
        the ratio of ordered predictions that we want to use to compute the metrics

    Returns
    -------------
    P: float
        the precision on the sick class
    R: float
        the recall on the sick class
    F1: float
        the F1 score on the sick class
    """
    combined = list(zip(y_sick_scores.ravel(),y_test.ravel()))
    combined.sort(key=lambda x: x[0],reverse=True)

    twenty_pct_limit = round(top_ratio*len(combined))
    top = combined[:twenty_pct_limit]

    y_pred = [1 if x[0] > 0.5 else 0 for x in top ]
    y_true = [x[1] for x in top]
    R = 100*sklearn.metrics.recall_score(y_true,y_pred)
    P = 100*sklearn.metrics.precision_score(y_true,y_pred)
    F1 = 100*sklearn.metrics.f1_score(y_true,y_pred)
    return P,R,F1

def distinct_date_split(x,y,dates,k = 5, balanced = True, shuffle = True):
    """
    Provides a list of k-tuples of train and test indices such that the folds all contain distinct dates.
    
    Parameters
    -------------
    x,y : numpy ndarrays
        the values and target
    dates : numpy ndarray
        an array of dates with dimension (len(x),1)
    k : int, default 5
        number of splits 
    balanced : boolean, default True
        should be set to True if we want the training set to have balanced classes
    shuffle : boolean, default True
        should be set to True if we want the train and test indices to be shuffled

    Returns
    -------------
    splits: list 
        a list of pair train-test indices
    """
    dates = pd.Series(dates)
    distinct_days = dates.unique()
    # We shuffle the days for some randomness in our folds
    if(shuffle):
        np.random.shuffle(distinct_days)

    day_2_fold = {key: value%k for (value, key) in list(enumerate(distinct_days))} 
    folds = dates.map(day_2_fold).values

    
    healthy_i_in_dataset = np.argwhere(y == 0).ravel()
    sick_i_in_dataset = np.argwhere(y == 1).ravel()

    # splits contains k tuple of train_indices and test_indices
    splits = []
    for f in range(k):
        train_index = np.argwhere(folds != f).ravel()
        test_index = np.argwhere(folds == f).ravel()

        if(balanced):
            # to balance the two classes
            healthy_i_in_train = np.intersect1d(healthy_i_in_dataset,train_index)
            sick_i_in_train = np.intersect1d(sick_i_in_dataset,train_index)

            sampled_healthy_i = np.random.choice(healthy_i_in_train, size=len(sick_i_in_train), replace = False)
            balanced_train_index = np.union1d(sick_i_in_train,sampled_healthy_i)
            
            train_index = balanced_train_index
        if(shuffle):
            # so that they are shuffled
            train_index = np.random.permutation(train_index)
            test_index = np.random.permutation(test_index)

        splits.append((train_index,test_index))
    return splits

def analyse_clustering(y,clusters):
    """
    Given true labels and cluster predictions the function displays the details of the repartition 
    of the different classes inside the binary clusters and therefore tries to compute a precision 
    and recall for the clustering.

    Parameters
    -------------
    y: numpy ndarray
        true labels
    clusters: numpy ndarray
        corresponding cluster labels

    Returns
    -------------
    df: pandas Dataframe
        containing all the details of the clustering analysis

    """
    analysis_df = pd.DataFrame(np.hstack((y.reshape(-1,1),clusters.reshape(-1,1))),columns=['Labels','Cluster'])
    analysis_df['Counts'] = 1
    synthesis = analysis_df.groupby(['Cluster','Labels']).sum()
    cluster_counts = analysis_df.groupby(['Cluster']).sum()[['Counts']]
    cluster_counts.columns = ['Cluster_counts']
    label_counts = analysis_df.groupby(['Labels']).sum()[['Counts']]
    label_counts.columns = ['Label_counts']

    merged = synthesis.merge(cluster_counts, left_index=True, right_index=True).merge(label_counts, left_index=True, right_index=True)

    merged['Cluster proportion'] = 100*merged['Counts']/merged['Cluster_counts']
    merged['Label proportion'] = 100*merged['Counts']/merged['Label_counts']

    # Then we determine the healthy and sick cluster
    cluster_0_ratio_sick = np.sum(y[clusters == 0])/len(y[clusters == 0])
    cluster_1_ratio_sick = np.sum(y[clusters == 1])/len(y[clusters == 1])
    
    healthy_cluster = 0 if(cluster_1_ratio_sick > cluster_0_ratio_sick) else 1
    sick_cluster = 1^healthy_cluster
    merged = merged.reset_index()
    merged['Cluster'] = merged['Cluster'].map({healthy_cluster:'healthy',sick_cluster:'sick'})
    merged = merged.set_index(['Cluster','Labels'])
    merged['Desc'] = ''
    merged.set_value(('healthy',0.0),'Desc','True negatives')
    merged.set_value(('healthy',1.0),'Desc','False negatives')
    merged.set_value(('sick',0.0),'Desc','False positives')
    merged.set_value(('sick',1.0),'Desc','True positives')

    tp = merged.query("Desc == 'True positives'")['Counts'].values[0]
    fn = merged.query("Desc == 'False negatives'")['Counts'].values[0]
    fp = merged.query("Desc == 'False positives'")['Counts'].values[0]

    precision = round(100*tp/(tp+fp),3)
    recall = round(100*tp/(tp+fn),3)
    print('Precision = {}%\tRecall = {}%'.format(precision,recall))
    return merged[['Desc','Counts','Label proportion','Cluster proportion']]

def analyse_prediction(y,preds):
    """
    Computes a dataframe givign the different emtrics of a binary classification 
    (Precision, Recall, F1 and support)

    Parameters
    -------------
    y: numpy ndarray
        true labels
    preds: numpy ndarray
        corresponding predicted labels

    Returns
    -------------
    df: pandas Dataframe
        containing all the details of the prediction analysis
    """
    precision, recall, f1, support = sklearn.metrics.precision_recall_fscore_support(y, preds)
    df = pd.DataFrame()
    df['Precision'] = precision
    df['Recall']=recall
    df['F1 score']=f1
    df = df.apply(lambda x: round(100*x,3))
    df['Support']= support
    return df

def get_recall_for_precision(y_test,y_proba_sick,prec_thresh):
    """
    Finds the highest recall level that can be achived 
    for a given precision level.

    Parameters
    -------------
    y_test: numpy ndarray
        true labels
    y_proba_sick: numpy ndarray
        probability of each sample to be sick
    prec_thresh: float
        the given level of precision that we are interested in

    Returns
    -------------
    R: float   
        the max recall for that precision level
    thr: float
        the cutoff probability to achieve such level
    """
    precision, recall, threshold = sklearn.metrics.precision_recall_curve(y_test, y_proba_sick)
    
    # we return the highest precision that can be achieved for the given precision
    indices = np.argwhere(precision>=prec_thresh)

    if(len(indices)==0):
        # the precision level is never obtained therefore the threshold is 1 and the recall is 0
        return 0,1
    else:
        max_i = indices[0][0]
        R = recall[max_i]
        thr = threshold[max_i-1]
        return R,thr
    
def recalls_for_prec_list(y_test,y_proba_sick,prec_thresh_list):
    """
    Finds the highest recall level that can be achived 
    for multiple precision levels

    Parameters
    -------------
    y_test: numpy ndarray
        true labels
    y_proba_sick: numpy ndarray
        probability of each sample to be sick
    prec_thresh_list: list(float)
        list of precision levels we are interested in

    Returns
    -------------
    Recalls: list(float)
        the list of max recall for each precision levels
    thresholds: float
        the list of cutoff probabilities to achieve such levels
    """
    results = [get_recall_for_precision(y_test,y_proba_sick,p) for p in prec_thresh_list]
    recalls = np.array([x[0] for x in results])
    thresholds = np.array([x[1] for x in results])

    return recalls,thresholds

def partial_auc(x_train, x_test,y_train, y_test,clf,min_precision=0.7):
    """
    Returns an estimate of the partial area under the curves. It asks for a minimum precision 
    and will only compute the auc for the curve where the precision ranges between 1 
    and min_precision

    Parameters
    -------------
    x_train: numpy ndarray
        training inputs
    x_test: numpy ndarray
        testing inputs
    y_train: numpy ndarray
        training target
    y_test: numpy ndarray
        true labels
    clf: sklearn pipeline or any model we can call fit() and predict_proba() on
        the classifier
    min_precision: float, default 0.7
        the minimum precision up to where we compute the area

    Returns
    -------------
    auc: float
        the area under the curve
    """
    clf.fit(x_train,y_train)
    y_proba_sick = clf.predict_proba(x_test)[:,1]
    precision, recall, _ = sklearn.metrics.precision_recall_curve(y_test, y_proba_sick)
    
    p_index = np.where(precision>0.7)
    p_precision = precision[p_index]
    p_recall = recall[p_index]
    sklearn.metrics.auc(p_recall,p_precision)
    return sklearn.metrics.auc(recall,precision)

def cross_validate_auc(x,y,clf,splits,min_precision=0.7):
    """
    Returns an unbiased estimate of the partial auc by cross validating 
    it over the different splits

    Parameters
    -------------
    x: numpy ndarray
        ML inputs
    y: numpy ndarray
        ML targets
    clf: sklearn pipeline or any model we can call fit() and predict_proba() on
        the classifier
    splits: 
        a list of pair train-test indices
    min_precision: float, default 0.7
        the minimum precision up to where we compute the area

    Returns
    -------------
    m_auc: float
        the mean pAUC
    s_auc: float
        the std pAUC
    """
    auc = []
    for i,(train_index,test_index) in enumerate(splits):
        x_train, x_test,y_train, y_test = x[train_index], x[test_index], y[train_index], y[test_index]
        auc.append(partial_auc(x_train, x_test,y_train, y_test,clf))
    return np.mean(auc),np.std(auc)

def custom_GridSearchCV(x,y,dates,estimator,param_grid,cv=5,min_precision=0.7):
    """
    Performs a grid search of optimal parameters by trying each possible combination of proposed parameters and 
    performing a cross validation on the partial AUC for each combination

    Parameters
    -------------
    x: numpy ndarray
        ML inputs
    y: numpy ndarray
        ML targets
    dates: numpy ndarray
        an array of dates with dimension (len(x),1) of the day_0 of imputs x
    estimator: param -> sklearn pipeline
        a function that generates a classification pipeline when passed a parameter
    param_grid: dict(str -> list())
        a dict that gives to each possible parameter name the list of different 
        values we wish to try
    cv: int, default 5
        number of folds to use to cross validate
    min_precision: float, default 0.7
        the minimum precision up to where we compute the area

    Returns
    -------------
    result_dict: dict
        a dictionnary containing the mean prAUC and std prAUC for each combination in 
        result_dict['results'] and the best one in result_dict['best']
    """
    # we generate all the possible instantiation parameter dictionnaries
    items = sorted(param_grid.items())
    keys, values = zip(*items)
    
    splits = distinct_date_split(x,y,dates,cv)
    best = {'best_auc':0,'best_args':None}
    best_auc = 0
    best_args = None
    
    param_combinations = list(itertools.product(*values))
    n_iteration = len(param_combinations)
    scripts.utils.progress(0, n_iteration, suffix='Trying different combinations')
    results = {}
    
    for i,v in enumerate(param_combinations):
        kw_args = dict(zip(keys, v))
        clf = estimator(kw_args)
        mean_auc,std_auc = cross_validate_auc(x,y,clf,splits)
        if(mean_auc > best['best_auc']):
            best['best_auc'] = mean_auc
            best['best_args'] = kw_args
            
        results[str(kw_args)] = ({'mean_auc':mean_auc,'std_auc':std_auc})
        scripts.utils.progress(i+1, n_iteration, suffix='Trying different combinations')
    return {'results':results,'best':best}
    