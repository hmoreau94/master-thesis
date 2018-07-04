# CPE Failure Prediction

This repository contains the result of a Proof of Concept develloped by [Hugo Moreau](https://www.linkedin.com/in/hugomoreau/) (Msc in Communication Systems) in the scope of his Master Thesis at UPC Cablecom. It attempts to use the data owned and collected by UPC in order **to perform proactive servicing of CPE (Customer Premises Equipment)**. It is composed of two major phases:

* Data Collection (`P/L SQL`)
* Data Analysis and Machine Learning (`Python`)

# Structure
The structure of the repository reflects the different steps of the project. We chose to highlight only the content of folders that required some explanations.

```
├── analysis/									# Data Analysis and Machine Learning
│		├── Data/								# Data Samples and Saved objects
│		├── Distance_matrices/				# Distance matrices used for energy test (influence of weekends)
│		├── Final.ipynb						# Jupyter Notebook explaining all the protocol 
│		├── Final.html						# Html version of the notebook (doens't require jupyter)
│		└── scripts/   						# All python scripts used by the notebook
│       	├──  __init.py__					
│       	├──  __pycache__
│       	├──  energy_test_DP.py			# Influence of weekends on vectors
│       	├──  model_selection.py			# To select the optimal model (without plotting)
│       	├──  plot.py						# All functions generating plots
│       	├──  preprocessing.py			# Functions to put the data in format for ML
│       	└──  utils.py						# Utility functions and to import the data in python
├── archive/									# Archives ressearch notebooks
├── packages/									# P/L SQL packages
├── README.md
└── report/									# All sources relative to the thesis
```

## Scripts
We will now describe briefly the content of the scripts by enumerating functions they contain and by giving the purpose of each function

### `energy_test_DP.py`

- `get_ks_test_result`: Performs the Kolmogorov-Smirnov test on a list of measurement to detect whether the same measurement taken from two population can be considered as being sampled from distinct distributions.
- `energy_two_sample_large_dataset`: Similar to energy_two_sample_test but works on large data samples, so it will subsample the population and run the experiment multiple times to estimate test results.
- `energy_two_sample_test`: Performs the hypothesis testing: given two independent random samples in R^d, it will test whether we can reject the null hypothesis (the two samples are sampled from the same distribution) at a significance level alpha.
- `compute_energy_statistic`: Computes the energy statistic for two group of independent random samples of random vectors
- `compute_double_sum_eucl`: Computes sum of euclidean norms between each possible pair of vectors in groups designated by `indices_X` and `indices_Y`
- `get_resampled_indices`: Will compute the indices of the bootstrap samples from the pooled samples
- `find_p_value`: Computes the p-value obtained from the statistical test

### `model_selection.py`
- `get_cross_validated_metrics`: Computes cross validated classification metrics on the top_ratio prediction
- `get_metrics`: Returns a string synthesizing the classification performance for the top prediction. That is we order the predictions based on the probability that the sample belongs to the 'sick'(=1) class and then look only at this subset to compute our metrics.
- `distinct_date_split`: Provides a list of k-tuples of train and test indices such that the folds all contain distinct dates.
- `analyse_clustering`: Given true labels and cluster predictions the function displays the details of the repartition of the different classes inside the binary clusters and therefore tries to compute a precision and recall for the clustering.
- `analyse_prediction`: Computes a dataframe givign the different emtrics of a binary classification (Precision, Recall, F1 and support)
- `get_recall_for_precision`: Finds the highest recall level that can be achived for a given precision level.
- `recalls_for_prec_list`: Finds the highest recall level that can be achived for multiple precision levels
- `partial_auc`:  Returns an estimate of the partial area under the curves. It asks for a minimum precision and will only compute the auc for the curve where the precision ranges between 1 and min_precision
- `cross_validate_auc`: Returns an unbiased estimate of the partial auc by cross validating it over the different splits
- `custom_GridSearchCV`: Performs a grid search of optimal parameters by trying each possible combination of proposed parameters and performing a cross validation on the partial AUC for each combination

### `plot.py`
- `plot_difference`: Plots the distribution of a variable for week and weekend
- `single_roc_curve`: Compute a cross validated ROC curve for a given model
- `roc_curves	`: Draws all the roc_curves for each parameter for a given pipeline generator
- `single_precision_recall_curve`: Draws a precision recall curve and evaluates the maximum recall that the model can achieves for different precision level
- `precision_recall_curves`:  Graphically compares multiple models using precision recalls curves
- `compare_distrib`: Allows us to compare the distribution of variables depending on their class in order to see how the classifier could potentially use a variable to discriminate into one group or another.
- `plot_gap_performance`: Plots the Recall Precision curve for different time gaps between the training and testing.
- `weekday_only_pr_curve`: Plots a PR curve for a given model and weekday such that both training and testing are only composed of the same weekday
- `weekday_influence`: Plots precision recall curves for each weekday to analyse whether training and testing on the same weekday yields higher performances
- `look_at_joint_dist`: This function will create as many plots as there are elements in identical_set to show the joint distribution of the key with each element of the set
- `silhouette_analysis`: Plots a silhouette analysis for the clustering of a given dataset using K-means and for different number of clusters.
- `get_single_silhouette`: Will draw a silhouette for a given dataset
- `plot_silhouette_score`: Allows us to plot silhouette scores and sum of squared errors for K-means clustering on different number of cluster in order to determine the most appropriate number of clusters.
- `plot_silhouette_score_different_PCA`: Depending on the ratio of retained var we plot the silhouette score over different number of clusters

### `preprocessing.py`
- `encode_categorical`: Will encode selected columns of the feature vector dataframe using a one hot encoding
- `convert_to_binary_labels`: Converts the labels into binary. (It assumes that everything in y set to None belongs to class 0 and the rest to class 1)
- `impute_missing`: Will return a dataframe with no missing values.
- `remove_features`: In order to remove the features spotted in the initial analysis as duplicated or highly correlated with another one.
- `get_balanced_classes`: In order to balance the dataset. Will return a randomly shuffled subsample of the dataset that subsamples the positive class (healthy)
- `nearZeroVar`: Diagnoses features that have one unique value (i.e. are zero variance predictors) or predictors that are have both of the following characteristics: 
	* very few unique values relative to the number of samples 
	* the ratio of the frequency of the most common value to the frequency of the second most common value is large.
- `are_identical`: Given two columns in a pandas dataframe it will return whether the two columns are exactly identicals or not.
- `add_pair_to_dict`: will add to dic a pair of strings such that if one of the element of the pair is in the dic keys the other will be added to the list stored in the dict. Otherwise it will create a list corresponding to the first element of the pair and add the second element to this list
- `find_correlation`: Given a numeric pd.DataFrame, this will find highly correlated features, and return a list of features to remove.
- `compare_candidate_identical`: Helpers to look at joint distribution of two variables. It will plot principal against each of the variables in secondaries for each time aggregate denoted by suffixes.

### `utils.py`
- `progress`: Shows the progress of a given action, using a progress bar
- `get_longest_date_seq`: Returns the longuest sequence (consecutive) of dates as a list of dates.
- `usable_data`: To get data that is directly usable for our machine learning, it will import the sample, extract the ML usable data, convert labels to binary and finally encode the categorical features.
- `excel_to_df`: This function helps us read an excel file efficiently into a Pandas dataframe: if a serialized version exists it will load it from there to avoid processing it twice, if it isn't the case it will process it and serialize it.
- `import_sample`: Will import the sample of data from a source xlsx file, and will dump it to increase future import performances. Among others: it will
	* convert column names to lowercase
	* convert categorical features to categorical
	* convert the dates to correct format
	* translate the hardware model to a unique index
	* get rid of dates during which there is not a single CPE
- `get_ml_data`: Using the extracted dataframe, this function will return a dataframe that correspond to the feature vectors and a numpy array corresponding to the classes.

## Packages

### ```check_queries.sql```
Contains a backup of queries that can help check the content of our buffer tables and therefore check the state of the database

### ```package_DMP.sql```
This package contains all the necessary procedure to be able to perform Data Collection for CPE Failure prediction. It was designed for the database architecture of UPC Cablecom in Feb-Aug 2018 and the following description will make the assumption of such architecture. This particular set of procedure is supposed to run on DMP and will fill in temprorary windows that shall be unloaded by ```CPE_FAIL_DETECTION_POC.Main_Proc``` in DMT (package described by ```package_DMT.sql```).The package assumes that it is ran every day as it works with a rolling window mechanism.

### ```package_DMP_init.sql```
As a package containing references to non-existant tables cannot compile we need an external package to help us create such tables. This package will help to relaunch the POC once tables have been deleted. 

### ```package_DMT.sql```
This package contains all the necessary procedure to be able to perform Data Collection for CPE Failure prediction. It was designed for the database architecture of UPC Cablecom in Feb-Aug 2018 and the following description will make the assumption of such architecture. This particular set of procedure is supposed to run on DMP and will fill in temprorary windows that shall be unloaded by ```CPE_FAIL_DETECTION_POC.Main_Proc``` in DMT (package described by ```package_DMT.sql```). The package assumes that it is ran every day as it works with a rolling window mechanism.

### ```package_DMT_init.sql```
As a package containing references to non-existant tables cannot compile we need an external package to help us create such tables. This package will help to relaunch the POC once tables have been deleted. 

### ```package_error_logging.sql```
This package contains the procedures that are used in order to log with autonomous transactions events. It should be declared both on the test and production datamarts (DMP and DMT).

### Usage
Here we will describe how to use these packages to collect data. First we discuss what package should be declared in what datamart: 

- DMT:
	- `package_DMT_init.sql`
	- `package_DMT.sql`
	- `package_error_logging.sql`
- DMP:
	- `package_DMP_init.sql`
	- `package_DMP.sql`
	- `package_error_logging.sql`	

Then because these package inductively construct a vector then need to be initialiazed. We assume that the error logging package will not be deleted (should it be the simplicity of the only table it relies on should allow anyone to recreate it following the instructions given in the package `package_error_logging.sql `).

```
-- On DMP
BEGIN
	CPE_FAIL_DETECTION_POC_INIT.initialize;
	CPE_FAIL_DETECTION_POC.init;
	CPE_FAILURE_ERROR_LOG.reset; 
	HUMOREAU.CPE_FAIL_DETECTION_POC.MAIN_PROC;
END;
```

```
-- On DMT
BEGIN
	CPE_FAIL_DETECTION_POC_INIT.initialize;
	CPE_FAILURE_ERROR_LOG.reset;
	HUMOREAU.CPE_FAIL_DETECTION_POC.MAIN_PROC;
END;

```

Now we can always run the `HUMOREAU.CPE_FAIL_DETECTION_POC.MAIN_PROC` on DMP and then on DMT everyday in order to collect data.

