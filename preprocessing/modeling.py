import polars as pl
import pandas as pd
import numpy as np
import preprocessing.transform_data as td
from sklearn.linear_model import RidgeClassifierCV, LogisticRegression, LogisticRegressionCV, SGDClassifier
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.pipeline import make_pipeline
from sklearn import metrics
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import RandomOverSampler, SMOTE
from sklearn.tree import DecisionTreeClassifier


def fit_predict_evaluate(classifier, X_train, y_train, X_test, y_test):
    classifier.fit(X_train, y_train)
    y_pred_train = classifier.predict(X_train)
    print("Performance on train data: ")
    print("accuracy: ",metrics.accuracy_score(y_train, y_pred_train))
    print("recall: ",metrics.recall_score(y_train, y_pred_train))
    print("F1: ",metrics.f1_score(y_train, y_pred_train))
    print(metrics.confusion_matrix(y_train, y_pred_train))

    y_pred = classifier.predict(X_test)
    print("Performance on test data: ")
    print("accuracy: ",metrics.accuracy_score(y_test, y_pred))
    print("recall: ",metrics.recall_score(y_test, y_pred))
    print("F1: ",metrics.f1_score(y_test, y_pred))
    print(metrics.confusion_matrix(y_test, y_pred))