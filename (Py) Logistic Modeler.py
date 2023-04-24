# -*- coding: utf-8 -*-
"""
Created on Mon Jan 21 21:01:06 2019

Take a category pivot file (straight up) as input.
Select a category to serve as the dependent variable y
run against all events. Optional regex to narrow the X variables

@author: Gerardo
"""

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

ivFile = pd.read_csv('IVref.csv')
dvFile = pd.read_csv('DVref.csv')

#Remove columns matching this regex. If None, do nothing
keepCats = None
dropCats = None
#Pick percent of data to be used for training.
test_pct = .8
#Perform RFE?
rfe = None


## Y vector
Y = dvFile['LOSS']

## X Matrix (removes "id" column)
X = ivFile.iloc[:,4:]
X.fillna(0, inplace=True)


#print(Y[:10])
#print(X[:10])


if keepCats:
    X = X.loc[:,X.columns.str.contains('Effort')]
if dropCats:
    X = X.loc[:,~X.columns.str.contains('Churn')]

print(X.head())

cols = X.columns

##Remove Low Variance (if Needed)
from sklearn.feature_selection import VarianceThreshold

sel = VarianceThreshold(threshold=0.005)
sel = sel.fit(X.values)
keep = sel.get_support(indices=True)
feature_names = [cols[idx] for idx, _ in enumerate(cols) if idx in keep]
X = sel.transform(X)

removed_features = list(np.setdiff1d(cols, feature_names))
print("Found {} low-variance columns. {} Features remain".format(len(removed_features),len(feature_names)))

Xf = pd.DataFrame(data=X, columns=feature_names)



##RFE - Recursive Feature Elimination
## Repeatedly fit a model and choose the best/worst performing feature,setting the feature aside and then repeating the process
from sklearn.feature_selection import RFE
if rfe:
    model = LogisticRegression(penalty='l2', fit_intercept=True, solver='lbfgs',max_iter=500)
    rfe = RFE(model, 30, verbose = 3)
    fit = rfe.fit(Xf.values, Y)
    keep = fit.support_
    print("Num Features: %d" % fit.n_features_)
    print("Selected Features: %s" % keep)
    print("Feature Ranking: %s" % fit.ranking_)
    Xf = Xf.loc[:,keep]
    Xf.to_csv("Top10_FeaturesRFE.csv", index=False)
    rfe_features = Xf.columns.tolist()
    print(rfe_features)



##Training vs Testing split
from sklearn.model_selection import train_test_split
X_train, X_test, Y_train, Y_test = train_test_split(Xf, Y, test_size=test_pct, random_state=0)

import statsmodels.api as sm
import statsmodels.formula.api as smf  
X_train = sm.add_constant(X_train)
X_test = sm.add_constant(X_test)

print('----------------------------------------------------------------')
logit_mod = sm.Logit(Y_train, X_train)
logit_result = logit_mod.fit()

summary = logit_result.summary2()
print(summary)
summary.tables[1].to_csv('{}'.format("Logit_Model_LOSS.csv"))


from sklearn.metrics import classification_report, confusion_matrix

Y_pred = logit_result.predict(X_test)

pred = list(map(round, Y_pred))
print(classification_report(Y_test, pred))
confusion_matrix = confusion_matrix(Y_test, pred)
print(confusion_matrix)