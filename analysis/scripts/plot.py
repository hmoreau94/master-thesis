# -*- coding: utf-8 -*-

"""
    Module containing all the functions that are used to generate plots. 
    mainly composed of three groups of functions, those that are used to 
    do the Analysis of the Weekends influence on the vectors, the clustering 
    analysis and the classification analysis
"""
__author__ = "Hugo Moreau"
__email__ = "hugo.moreau@epfl.ch"
__status__ = "Prototype"

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import math

import sklearn
from sklearn.model_selection import train_test_split
from sklearn.model_selection import KFold
from scipy import interp

import scripts.model_selection
from scripts.preprocessing import *
from scripts.model_selection import *
from scripts.utils import *

### --------------------------------------------------------------------------------------------
### ----------------------------------------Week-end analysis-----------------------------------
### --------------------------------------------------------------------------------------------
def plot_difference(column_name,week_df,weekend_df,ratio_pop_show=0.9,n_bins=1000):
    """
    Plots the distribution of a variable for week and weekend

    Parameters
    -------------
    column_name : String
        the variable that we wish to observer
    week_df,weekend_df : pandas DataFrame
        panda dataframes that contain column_name as a col and 
        respectively give the samples from the week end and week
    ratio_pop_show : float, default 0.9
        the ratio of the total population that we wish to display in order to hide the outliers.
    n_bins : int, default 1000  
        the number of bins to use to build the histogram for continuous variables
    """
    week_serie = week_df[column_name].dropna()
    weekend_serie = weekend_df[column_name].dropna()
    
    f, axes = plt.subplots(1, 2, figsize=(18, 5), sharex=True , sharey =True)
    axes[0].set_title("Weekend")
    axes[1].set_title("Week")
        
    # Then we compute the x axis limit such that ratio_pop_show is shown on the graph
    lower_quantile = (1-ratio_pop_show)/2
    upper_quantile = 1-lower_quantile
    lower_x = min(week_serie.quantile(lower_quantile),weekend_serie.quantile(lower_quantile))
    upper_x = max(week_serie.quantile(upper_quantile),weekend_serie.quantile(upper_quantile))
    if('MISS' in column_name):
        week_serie.value_counts().apply(lambda x: x/week_serie.count()).sort_index().plot(kind='bar',title='Week',color="red",ax=axes[1])
        weekend_serie.value_counts().apply(lambda x: x/weekend_serie.count()).sort_index().plot(kind='bar',title='Weekend',color="skyblue",ax=axes[0])
    else :
        axes[0].set_xlim(lower_x,upper_x)
        axes[1].set_xlim(lower_x,upper_x)
        sns.distplot(weekend_serie,bins=n_bins,color="skyblue",ax=axes[0],norm_hist=True)
        sns.distplot(week_serie,bins=n_bins,color="red",ax=axes[1],norm_hist=True)
    f.suptitle('Distribution of {}'.format(column_name))
    f.show()

### --------------------------------------------------------------------------------------------
### ----------------------------------------Classification Analysis-----------------------------
### --------------------------------------------------------------------------------------------
def single_roc_curve(x,y,pipeline_generator,param,top_ratio = 0.15,ax=None,cv=5,distinct_date=False,dates = None):
    """
    Compute a cross validated ROC curve for a given model. 

    Parameters
    -------------
    x,y : numpy ndarrays
        data 
    pipeline_generator : param -> sklean pipeline
        a function that takes the model parameters to create an instantiation
    param: (depends on the way pipeline_generator is created)
        the parameter that need to be passed to the pipeline generator
    top_ratio: float, default 0.15 
        the ratio of prediction that we want to consider as our most imprtant prediction and on which metrics will be computed.
    ax: matplotlib.axes._subplots.AxesSubplot, optionnal
        to plot multiple Roc curves against each other. (can be left empty if we do not wish to do so)
    cv: int, default 5
        number of folds to consider to cross validate the curve and metrics
    distinct_date: bool, default False
        whether we wish to split the dataset in order to have distinct dates in the training and testing set
    dates: numpy array
        an array of dates (same length as x has rows)

    Returns
    -------------
    m: numpy ndarray
        contains 3 elements Precision Recall and F1-score of the model estimated over different folds

    """
    tprs = []
    aucs = []
    mean_fpr = np.linspace(0, 1, 100)
    metrics = np.zeros((cv,3))

    i = 0
    model = pipeline_generator(param)

    assert((distinct_date and dates is not None) or not distinct_date), 'distinct_date was True but no date provided'

    if(ax is None):
        # if we do not want to plot multiple roc curves side by side
        fig,ax = plt.subplots(1,1)

    # construct the splits depending on the strategy
    if(not distinct_date):
        kf = KFold(n_splits=cv)
        x,y = get_balanced_classes(x,y)
        splits = kf.split(x)
    else:
        splits =  distinct_date_split(x,y,dates,k=cv)

    for train, test in splits:
        probas_ = model.fit(x[train], y[train]).predict_proba(x[test])
        
        # Compute ROC curve and area the curve
        fpr, tpr, thresholds = sklearn.metrics.roc_curve(y[test], probas_[:, 1])
        tprs.append(interp(mean_fpr, fpr, tpr))
        tprs[-1][0] = 0.0
        roc_auc = sklearn.metrics.auc(fpr, tpr)
        aucs.append(roc_auc)
        ax.plot(fpr, tpr, lw=1, alpha=0.3,
                 label='ROC fold %d (AUC = %0.2f)' % (i, roc_auc))

        # compute the metrics on the top ratio
        metrics[i,:] = get_metrics(probas_[:, 1],y[test],top_ratio)

        i += 1

    # add the diagonal to show what a random clf would give
    ax.plot([0, 1], [0, 1], linestyle='--', lw=2, color='r',
         label='Luck', alpha=.8)

    mean_tpr = np.mean(tprs, axis=0)
    mean_tpr[-1] = 1.0
    mean_auc = sklearn.metrics.auc(mean_fpr, mean_tpr)
    std_auc = np.std(aucs)
    m = np.mean(metrics,axis = 0)

    t = 'Param = {}'.format(param)
    t += "\nP = {:.3f}%; R = {:.3f}%; F1 = {:.3f}%".format(m[0],m[1],m[2])

    ax.plot(mean_fpr, mean_tpr, color='b',
             label=r'Mean ROC (AUC = %0.2f $\pm$ %0.2f)' % (mean_auc, std_auc),
             lw=2, alpha=.8)

    std_tpr = np.std(tprs, axis=0)
    tprs_upper = np.minimum(mean_tpr + std_tpr, 1)
    tprs_lower = np.maximum(mean_tpr - std_tpr, 0)

    # confidence interval
    ax.fill_between(mean_fpr, tprs_lower, tprs_upper, color='grey', alpha=.2,
                     label=r'$\pm$ 1 std. dev.')

    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    
    ax.set_xlabel('False Positive Rate')
    ax.set_ylabel('True Positive Rate')
    ax.set_title(t)
    ax.legend(loc="lower right")
    return m

def roc_curves(param_list,pipeline_generator,x,y,title,top_ratio,dates=None,distinct_date=False,cv=5):
    """
    Draws all the roc_curves for each parameter for a given pipeline generator
    
    Parameters
    -------------
    param_list: list (type of elements depend on the way pipeline generator is constructed)
        the lsit of parameters that we wish to pass to the pipeline constructer
    pipeline_generator: param -> sklearn pipeline
        a constructor that gives us a classification pipeline when given a param (and possibly as a pair)
    x,y: numpy ndarrays
        the data
    title: string
        title of the model
    top_ratio: float
        the ratio of top predictions we wish to use
    dates: ndarray, optionnal
        day_0 of the inputs 
    distinct_date: boolean, defautl False
        can be set to true if we wish the cross validation to take place on distinct date between folds
    cv: int, default 5
        the number of folds used to cross validate the results

    Returns
    -------------
    title: string
        The title of the model that is being tested
    P: float
        the optimal precision percentage
    R: float
        the optimal recall percentage
    F1: float
        the optimal F1-score
    opt_param: depends on the way the pipeline generator is constructed
        the optimal parameter(s) that yields optimal metrics.
    """
    n_subplots=len(param_list)
    fig, axes = plt.subplots(ncols=n_subplots,sharex = True,sharey=True,figsize=(5*n_subplots, 5))
    scripts.utils.progress(0, n_subplots, suffix='Generating subplots')

    results = np.zeros((n_subplots,3))

    if(n_subplots == 1):
        axes = [axes]
    for i,p in enumerate(param_list):
        results[i,:] = single_roc_curve(x,y,pipeline_generator,p,top_ratio=top_ratio,ax=axes[i],cv=cv,distinct_date=distinct_date,dates = dates)
        scripts.utils.progress(i+1, n_subplots, suffix='Generating subplots')


    # we get the max F1 score to return the optimal metrics
    opt_index = np.argmax(results[:,2])
    opt_param = param_list[opt_index]
    P,R,F = results[opt_index,:].tolist()

    fig.suptitle(title)
    plt.subplots_adjust(top=0.85)
    fig.show()
    return title,P,R,F,opt_param


def single_precision_recall_curve(clf, x, y, dates, title,prec_thresh_list=[0.7,0.8,0.9],cv=5, balanced = True, shuffle = True,ax=None,plot_var=True):
    """
    Draws a precision recall curve and evaluates the maximum recall that the model can achieves for different precision level

    Parameters
    -------------
    clf: sklearn pipeline 
        an instantiated classification pipeline
    x,y: numpy ndarrays
        ML ready data to be used for the task
    dates: numpy array
        the day_0 of each vector in the input dataset
    title: str
        the title we wish to give the graph
    prec_tresh_list: list of float, default 0.8
        the precision levels that we wish to compute the max recall for
    cv: int, default 5
        number of folds to use for the cross-validation
    balanced: boolean, default True
        can be set to true if we wish to balance each fold in the training set
    shuffle: boolean ,default true
        can be set to true if we wish to shuffle the dates that are in each fold.
    ax: matplotlib.axes._subplots.AxesSubplot, optional
        if we wish to plot multiple such curves against each other 
    plot_var: boolean
        can be set to false if we wish to draw only the overall curve and not each fold's

    Returns 
    -------------
    arr: np array
        contains the recall levels for specified precision threshold and the overall auc
    """
    if(ax is None):
        # if we do not want to plot multiple roc curves side by side
        fig,ax = plt.subplots(1,1)

    splits = scripts.model_selection.distinct_date_split(x,y,dates,cv,balanced,shuffle)
    recall_levels = np.zeros((cv,len(prec_thresh_list)))
    threshs = np.zeros((cv,len(prec_thresh_list)))
    
    y_real = []
    y_scores = []
    
    for i,(train_index,test_index) in enumerate(splits):
        x_train, x_test,y_train, y_test = x[train_index], x[test_index], y[train_index], y[test_index]
        clf.fit(x_train,y_train)
        y_proba_sick = clf.predict_proba(x_test)[:,1]
        if(plot_var):
            precision, recall, threshold = sklearn.metrics.precision_recall_curve(y_test, y_proba_sick) 
            lab = 'Fold {:d} AUC = {:.4f}'.format(i+1,sklearn.metrics.auc(recall,precision))
            
            ax.step(recall, precision,alpha=0.2,where='post',label=lab)
        
        #store them 
        y_real.append(y_test)
        y_scores.append(y_proba_sick)
        recalls_for_i,thresholds_for_i = scripts.model_selection.recalls_for_prec_list(y_test,y_proba_sick,prec_thresh_list)
        recall_levels[i,:] = recalls_for_i
        threshs[i,:] = thresholds_for_i
    
    # compute the overall
    y_real = np.concatenate(y_real)
    y_scores = np.concatenate(y_scores)
    precision, recall, _ = sklearn.metrics.precision_recall_curve(y_real, y_scores)

    overall_auc = sklearn.metrics.auc(recall,precision)
    lab = 'Overall AUC = {:.4f}'.format(overall_auc)
    ax.step(recall, precision, label=lab,where='post',lw=2, color='black')

    ax.set_xlabel('Recall')
    ax.set_ylabel('Precision')
    ax.set_ylim([0.0, 1.05])
    ax.set_xlim([0.0, 1.0])

    recall_lev_s = ";".join(['{:.2f}%'.format(x) for x in 100*np.mean(recall_levels,axis=0)])
    threshold_estimates = ";".join(['{:.4f}'.format(x) for x in np.mean(threshs,axis=0)])
    title = title + ' [ ' + recall_lev_s+' ]\n[ '+threshold_estimates+' ]'
    ax.set_title(title)
    plt.subplots_adjust(top=0.85)
    ax.legend(loc='upper right', fontsize='small')

    if(ax is None):
        fig.show()

    return np.array(np.mean(recall_levels,axis=0).tolist() + [overall_auc])

def precision_recall_curves(param_list,pipeline_generator,x,y,title,prec_thresh,dates,cv=5):
    """
    Graphically compares multiple models using precision recalls curves

    Parameters
    -------------
    param_list: list (the type of elements depend on the way the pipeline generator is constructed)
        a list of parameters used to instante the classifcation pipeline
    pipeline_generator: param -> sklearn pipeline
        a function that generates a classification pipeline when passed a parameter
    x,y: np ndarrays
        ML ready data
    title: str
        the title of the serie of subplots
    prec_tresh: list of floats
        the list of precision threshold that we wish to use to evaluate the max recall
    dates: numpy ndarray
        day_0 of the vectors
    cv: int, default 5
        he number of folds to use to perform the cross validation

    Returns 
    -------------
    (as a list)
    title: str
        name of the model being trained
    opt_results:
        the max recall for each precision thresholds
    opt_param:
        the parameter(s) that yield the optimal results
    """
    n_subplots = len(param_list)

    # we will store for each parameter the max recall level and overall AUC
    results = np.zeros((n_subplots,len(prec_thresh)+1))
    scripts.utils.progress(0, n_subplots, suffix='Generating subplots')

    fig, axes =  plt.subplots(ncols=n_subplots,sharex = True,sharey=True,figsize=(5*n_subplots, 5))
    if(n_subplots == 1):
        axes = [axes]
    for i,p in enumerate(param_list):
        clf = pipeline_generator(p)
        t = 'Param = {}'.format(round(p,3))
        results[i,:] = single_precision_recall_curve(clf, x, y, dates,t,prec_thresh,cv,ax=axes[i])
        scripts.utils.progress(i+1, n_subplots, suffix='Generating subplots')

    prec_thresh_s = ";".join(['{:.2f}%'.format(100*x) for x in prec_thresh])
    enriched_title = title + ' (Avg R for P in [ '+prec_thresh_s+' ]'
    fig.suptitle(enriched_title)
    fig.show()

    # we get the optimal model based on AUC
    max_prec_thresh_i = np.argmax(prec_thresh)
    opt_index = np.argmax(results[:,1+max_prec_thresh_i])
    opt_param = param_list[opt_index]
    opt_results = results[opt_index,:].tolist()

    return [title] + opt_results + [opt_param]


def compare_distrib(var_to_explore, df, n_bins = 10):
    '''
    Allows us to compare the distribution of variables depending on their class 
    in order to see hwo the classifier could potentially use a variable to 
    discriminate into one group or another.

    Parameters
    -------------
    var_to_explore: str
        the name of the variable in the dataframe that we wish to look at
    df: pandas DataFrame
        the dataframe that contains all the data for the given variable as 
        well as a column 'sick' that is set to 1 or 0 depending on the target (the class)
    n_bins: int, default 10
        the number of bins used to create the histogram
    '''
    binned_name = var_to_explore + '_binned'
    min_ = df[var_to_explore].min()
    max_ = df[var_to_explore].max()
    df[str(binned_name)] = pd.cut(
        df[var_to_explore], n_bins, include_lowest=True)

    cnt = pd.DataFrame()
    cnt['sick'] = df[df['sick'] == 1][binned_name].value_counts()
    cnt['healthy'] = df[df['sick'] == 0][binned_name].value_counts()
    cnt = cnt.apply(lambda x: round(100*x/x.sum(), 3)).sort_index()

    plt.style.use('ggplot')
    ind = np.array([i for i, _ in enumerate(cnt.index)])
    width = 0.35
    plt.bar(ind, cnt['sick'], width, label='sick')
    plt.bar(ind+width, cnt['healthy'], width, label='healthy')
    plt.xticks(ind + width / 2, cnt.index)
    plt.legend(loc='best')
    plt.xticks(rotation=90)
    plt.title(var_to_explore)
    plt.show()

def plot_gap_performance(x,y,dates,model,gaps,ratio_testing=0.25):
    """
    Plots the Recall Precision curve for different time gaps between the training and testing.
    
    Parameters
    -------------
    x,y: np ndarrays
        ML ready data
    model: sklearn pipeline or model on which we can call fit()
        the machine learning model we wish to evaluate
    gaps: list(int)
        list of time gaps (as number of days) between the last training date and testing date
    ratio_testing: float, default 0.25
        the ratio of usable days we wich to use in order to 
        have a meaningful performance evaluation
    """
    dates_to_consider = get_longest_date_seq(dates,verbose=False)

    max_gap = max(gaps)
    usable_days = len(dates_to_consider) - max_gap
    testing_days = math.floor(ratio_testing*usable_days)
    training_days = usable_days - testing_days

    scripts.utils.progress(0, testing_days)

    # generate training sets.
    day_0 = dates_to_consider[0]
    training_sets_dates = [[day_0 + timedelta(days=first_day_delta) +
                            timedelta(days=i) for i in range(0, training_days)]
                           for first_day_delta in range(0, testing_days)]

    dates_pd = pd.to_datetime(dates)

    # to store the results
    true_y = {g: [] for g in gaps}
    score_y = {g: [] for g in gaps}
    i = 1

    for tr_d in training_sets_dates:
        tr_mask = dates_pd.isin(tr_d)
        x_training = x[tr_mask]
        y_training = y[tr_mask]

        # fit the model
        model.fit(x_training, y_training)

        # print the progress
        scripts.utils.progress(i, testing_days)
        i+=1

        for g in gaps:
            te_d = tr_d[-1] + timedelta(days=g + 1)
            te_mask = [dates_pd == te_d]
            x_testing = x[te_mask]
            y_testing = y[te_mask]

            # test the model and get predictions
            scores = model.predict_proba(x_testing)[:, 1]
            true_y[g].append(y_testing)
            score_y[g].append(scores)

    fig, ax = plt.subplots()
    for g in gaps:
        precision, recall, threshold = sklearn.metrics.precision_recall_curve(
            np.concatenate(true_y[g]), np.concatenate(score_y[g]))
        lab = 'Gap = {}d, AUC = {:.4f}'.format(
            g, sklearn.metrics.auc(recall, precision))
        ax.step(recall, precision, alpha=0.5, where='post', label=lab)

    ax.set_xlabel('Recall')
    ax.set_ylabel('Precision')
    ax.set_ylim([0.0, 1.05])
    ax.set_xlim([0.0, 1.0])
    ax.set_title(
        'Comparison of performance with\ndifferent training-testing time gaps')
    ax.legend(loc='upper right', fontsize='small')

def weekday_only_pr_curve(weekday_index,x_df,y,dates,clf,ax=None):
    """
    Plots a PR curve for a given model and weekday such that both 
    training and testing are only composed of the same weekday

    Parameters
    -------------
    weekday_index: int 
        the index of the weekday we wish to analyse (where 0 is monday and 6 sunday)
    x_df: pandas Dataframe
        the dataframe containing all the data along with the column name (if calling x_df.values 
        we would obtain ML ready data)
    y: numpy ndarray
        the target
    dates: numpy ndarray
        day_0 of the input vectors
    clf: instantiated pipeline 
        the model
    ax: matplotlib.axes._subplots.AxesSubplot, optional
        if we wish to plot multiple such curves against each other 
    """
    if(ax is None):
        # if we do not want to plot multiple roc curves side by side
        fig,ax = plt.subplots(1,1)

    col_name = 'wk_{:d}'.format(weekday_index)

    weekday_indices = x_df[col_name] == 1
    dates = dates[weekday_indices]
    x = x_df[weekday_indices].values
    y = y[weekday_indices]
    single_precision_recall_curve(clf, x, y, dates,'Weekday = {}\n'.format(weekday_index),ax=ax)

def weekday_influence(x_df,y,dates,clf):
    """
    Plots precision recall curves for each weekday to analyse whether training and 
    testing on the same weekday yields higher performances.

    Parameters
    -------------
    x_df: pandas Dataframe
        the dataframe containing all the data along with the column name (if calling x_df.values 
        we would obtain ML ready data)
    y: numpy ndarray
        the target
    dates: numpy ndarray
        day_0 of the input vectors
    clf: instantiated pipeline 
        the model
    """ 
    fig, axes =  plt.subplots(ncols=7,sharex = True,sharey=True,figsize=(5*7, 5))
    scripts.utils.progress(0, 7, "Creating subplots for each week day")
    for w_i in range(0,7):
        weekday_only_pr_curve(w_i,x_df,y,dates,clf,axes[w_i])
        scripts.utils.progress(w_i+1, 7, "Creating subplots for each week day")
    plt.subplots_adjust(top=0.85)
    fig.show()



### --------------------------------------------------------------------------------------------
### ----------------------------------------Correlation Analysis--------------------------------
### --------------------------------------------------------------------------------------------
def look_at_joint_dist(data,key,identical_set):
    """
    This function will create as many plots as there are elements in 
    identical_set to show the joint distribution of the key with each element of the set
    
    Parameters
    -------------
    data: pandas DataFrame 
        that contains the column named by the input variable 'key' and 'identical_set'
    key: str
        the main variable we want to compare to all others
    identical_set: list(str)
        the list of all the variables we wish to compare key to.
    """
    n_plots = len(identical_set)
    iden = list(identical_set)
    
    red = data[[key] + iden]
    pearson = red.corr()
    spearman = red.corr(method='spearman')
    
    width_single = 5
    height_single = 5
    n_rows = math.ceil(n_plots/4)
    n_cols = n_plots if (n_plots) <=4 else 4

    fig, axes = plt.subplots(nrows= n_rows,ncols=n_cols, figsize=(width_single*n_cols, height_single*n_rows))
    if(n_plots == 1):
        axes = [axes]
    fig.suptitle(key,fontsize=20)
    
    for i in range(n_plots):
        if(n_rows > 1):
            c = i % n_cols
            r = i//n_cols
            ax_ = axes[r,c]
        else:
            ax_=axes[i]
        
        sns.regplot(key, iden[i], data=data, ax=ax_)
        p = pearson.loc[key,iden[i]]
        s = spearman.loc[key,iden[i]]
        ax_.set_title('Pearson = {:.2f}, Spearman = {:.2f}'.format(p,s))
        
    fig.subplots_adjust(hspace=0.5)
    fig.show()

### --------------------------------------------------------------------------------------------
### ----------------------------------------Clustering Analysis---------------------------------
### --------------------------------------------------------------------------------------------
def silhouette_analysis(range_n_clusters,X):
    """
    Plots a silhouette analysis for the clustering of a given dataset 
    using K-means and for different number of clusters.

    Parameters
    -------------
    - range_n_clusters: list(int)
        Cluster numbers we wish to consider
    - X: numpy ndarray
        input dataset
    """

    # Create a grid of 3 columns to display plots neatly
    n_plots = len(range_n_clusters)
    n_rows = ceil(n_plots/3)
    n_cols = 3
    fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols,sharex=True, sharey=True)

    # tune the size of the grid
    fig.set_size_inches(7*3, 4*n_rows)
    to_print = ''

    for i,n_clusters in enumerate(range_n_clusters):
        # Initialize the clusterer with n_clusters value and a random generator
        # seed of 10 for reproducibility.
        clusterer = sklearn.cluster.MiniBatchKMeans(n_clusters=n_clusters, random_state=10, batch_size=1000)
        cluster_labels = clusterer.fit_predict(X)

        # plot the silhouette analysis
        ax = axes[i%3] if n_rows == 1 else axes[i//3][i%3]
        legend = get_single_silhouette(X,cluster_labels,n_clusters,ax)
        to_print += legend +'\n'

    print(to_print)
    fig.show()

def get_single_silhouette(X,cluster_labels,n_clusters,ax=None):
    """
    Will draw a silhouette for a given dataset

    Parameters
    -------------
    X: numpy ndarray
        the input data
    cluster_labels: numpy ndarray
        an array of labels of same length than X has rows
    n_clusters: int
        the number of clusters used
    ax: matplotlib.axes._subplots.AxesSubplot, optionnal
        when we wish to draw multiple subplots against each other
    """

    if(ax == None):
        fig, ax = plt.subplots(1,1)
        fig.set_size_inches(13, 7)

    ax.set_xlim([-0.5, 1])

    # The (n_clusters+1)*10 is for inserting blank space between silhouette
    # plots of individual clusters, to demarcate them clearly.
    ax.set_ylim([0, len(X) + (n_clusters + 1) * 10])

    # The silhouette_score gives the average value for all the samples.
    # This gives a perspective into the density and separation of the formed
    # clusters
    silhouette_avg = silhouette_score(X, cluster_labels)
    to_return = "For n_clusters = {:d}, the average silhouette_score is : {:.4f}".format(n_clusters,silhouette_avg)

    # Compute the silhouette scores for each sample
    sample_silhouette_values = silhouette_samples(X, cluster_labels)

    y_lower = 10
    for i in range(n_clusters):
        # Aggregate the silhouette scores for samples belonging to
        # cluster i, and sort them
        ith_cluster_silhouette_values = sample_silhouette_values[cluster_labels == i]

        ith_cluster_silhouette_values.sort()

        size_cluster_i = ith_cluster_silhouette_values.shape[0]
        y_upper = y_lower + size_cluster_i

        color = cm.spectral(float(i) / n_clusters)
        ax.fill_betweenx(np.arange(y_lower, y_upper),
                          0, ith_cluster_silhouette_values,
                          facecolor=color, edgecolor=color, alpha=0.7)

        # Label the silhouette plots with their cluster numbers at the middle
        ax.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

        # Compute the new y_lower for next plot
        y_lower = y_upper + 10  # 10 for the 0 samples

    ax.set_title(("n_clusters = %d" % n_clusters))
    ax.set_xlabel("The silhouette coefficient values")
    ax.set_ylabel("Cluster label")

    # The vertical line for average silhouette score of all the values
    ax.axvline(x=silhouette_avg, color="red", linestyle="--")
    ax.set_yticks([])  # Clear the yaxis labels / ticks
    ax.set_xticks([-0.9,-0.5,-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])
    
    if(ax == None):
        fig.show()
        print(to_return)

    return to_return;

def plot_silhouette_score(cluster_numbers,X):
    """
    Allows us to plot silhouette scores and sum of squared errors for K-means clustering on 
    different number of cluster in order to determine the most appropriate number of clusters.

    Parameters
    -------------
    cluster_numbers: list(int)
        a list of k we wish to use for k-means
    X: numpy ndarray
        data that is ready for clustering
    """

    inertias = []
    sc = []
    for k in cluster_numbers:
        k_mean = sklearn.cluster.MiniBatchKMeans(n_clusters=k, verbose=0, batch_size=1000)
        k_mean.fit(X)
        silhouette_avg = metrics.silhouette_score(X, k_mean.labels_, sample_size=1000)
        sc.append(silhouette_avg)
        inertias.append(k_mean.inertia_)

    red_inertias = [x/100000 for x in inertias]
    plt.figure(figsize=(23,10))
    plt.scatter(cluster_numbers,red_inertias)
    plt.plot(cluster_numbers, red_inertias)
    plt.xticks(cluster_numbers)
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('SSE (*10^5)')
    plt.title('Evolution of the sum of squared error w.r.t the # of clusters')
    plt.figure(figsize=(3,23))
    plt.show()

    plt.figure(figsize=(23,10))
    plt.scatter(cluster_numbers,sc)
    plt.plot(cluster_numbers, sc)
    plt.xticks(cluster_numbers)
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Silhouette average score')
    plt.title('Evolution of the silhouette score w.r.t the # of clusters')
    plt.figure(figsize=(3,23))
    plt.show()

def plot_silhouette_score_different_PCA(x,list_retained_var,list_cluster_numbers,sample = False):
    """
    Depending on the ratio of retained var we plot the silhouette score over different number of clusters

    Parameters
    -------------
    x: numpy ndarray
        data that is ready for clustering
    list_retained_var: list(float)
        list of ratio of the different variance that we wish to retain 
    list_cluster_numbers: list(int)
        list of different number of clusters we wish to consider
    sample: boolean, default False
        can be set to true if we wish to subsample the healthy class
    """
    if(sample):
        # we split our x into two populations
        vec_healthy = x_scaled[y_binary == 0]
        vec_sick = x_scaled[y_binary == 1]
        n_sick = len(vec_sick)

        # we select as many random indices from vec_healthy as we have routers that are sick
        sampled_indices = np.random.choice(len(vec_healthy), size=n_sick)

        sample_healthy = vec_healthy[sampled_indices]
        x_scaled = np.vstack((sample_healthy,vec_sick))

    # setup the figure
    plt.figure(figsize=(23,10))
    color = iter(cm.rainbow(np.linspace(0,1,len(list_retained_var))))
    plt.xticks(list_cluster_numbers)
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Silhouette average score')
    plt.title('Silhouette score w.r.t the # of clusters for different retained variance')

    for i,var in enumerate(list_retained_var):
        pca = PCA(var)
        X = pca.fit_transform(x_scaled)
        sc = []

        for k in list_cluster_numbers:
            k_mean = sklearn.cluster.MiniBatchKMeans(n_clusters=k, batch_size=1000)
            k_mean.fit(X)
            silhouette_avg = metrics.silhouette_score(X, k_mean.labels_, sample_size=1000)
            sc.append(silhouette_avg)

        c = next(color)
        plt.plot(list_cluster_numbers,sc,c=c,label='retained var = {}'.format(var))

    plt.legend()
    plt.show()