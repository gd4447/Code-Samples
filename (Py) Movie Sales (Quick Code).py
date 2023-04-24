# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
pd.set_option('display.expand_frame_repr', False)
#pd.set_option('display.max_colwidth', 25)

# %% [markdown]
#  #### Load Data

# %%
data = pd.read_excel('Sales Data.xlsx',sheet_name='SALE_DATA_TEST')
print(data.head(10))

# %% [markdown]
# #### Clean Data
# 
# 1. Remove PII data. Not relevant for this study, we can map later with id if needed.
#     * deleted 'first_name', 'last_name','email'
# 2. Compute the total cost of goods - order_cog
# 3. Compute the total sale price - order_price
# 4. Some column names have blank spaces, remove these
# 5. Genre column is too complex, translate elements into a list
# 6. Add column to compute promise-delivery gap
# 7. Remove reduntant columns

# %%
date = data.rename(columns=lambda x: x.strip(), inplace=True)
#data['email'] = data['email'].str.split('@').str[1]
df_clean = data

df_clean['order_cog'] = df_clean['cost_of_goods']*df_clean['unit_quantity']
df_clean['order_price'] = df_clean['sale_price_per_unit']*df_clean['unit_quantity']
df_clean['order_profit'] = df_clean['order_price']-df_clean['order_cog']

df_clean['Movie_Genre'] = df_clean['Movie_Genre'].str.split('|')
df_clean['Movie_Genre'] = df_clean['Movie_Genre'].fillna({i: [] for i in df_clean.index})

df_clean['delivery_gap'] = (df_clean['delivery_date'] - df_clean['promise_date']).dt.days
df_clean['delivery_time'] = (df_clean['delivery_date'] - df_clean['order_date']).dt.days

df_clean = data.drop(['email','first_name','last_name','delivery_date','Country','sale_price_per_unit','DVD_Title'], axis = 1)

print(df_clean.head())
df_clean.to_csv('att_sales_clean.csv', index=False)


# %% [markdown]
# #### Explode genres for analysis
#  

# %%
df_genre = df_clean.explode('Movie_Genre')

df_genre.to_csv('att_sales_genres.csv', index=False)

# %% [markdown]
# #### One-hot-encode Categorical variables for modeling
# 

# %%


from sklearn.preprocessing import MultiLabelBinarizer

state_dummies = pd.get_dummies(df_clean['State'], prefix=None, dummy_na=False, drop_first=False)
gender_dummies = pd.get_dummies(df_clean['gender'], prefix=None, dummy_na=False, drop_first=True)

print(df_clean.head())
mlb = MultiLabelBinarizer()
genre_dummies = pd.DataFrame(mlb.fit_transform(df_clean['Movie_Genre']), columns=mlb.classes_, index=df_clean.index)

df_model = pd.concat([df_clean, state_dummies, genre_dummies], axis=1)
df_model.drop(['id','State','Movie_Genre','gender','order_date','promise_date','order_cog','order_price'], axis=1, inplace=True)
df_model.fillna(0,inplace=True)
print(df_model.head())

feature_list = list(df_model.columns)
df_model.to_csv('df_model.csv', index=False)



# %% [markdown]
# #### Model

# %%
test_pct = .2

# %%
Y = df_model['order_profit']
X = df_model.drop('order_profit', axis=1)

# %%
from sklearn.model_selection import train_test_split
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=test_pct, random_state=0)

XY_train = pd.concat([Y_train, X_train], axis=1)
XY_test = pd.concat([Y_test, X_test], axis=1)

# %%
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, ConfusionMatrixDisplay, roc_auc_score

rf1=RandomForestRegressor(n_estimators=100, random_state = 123)

#Train the model using the training sets y_pred=clf.predict(X_test)
rf1.fit(X_train,Y_train)

Y_pred=rf1.predict(X_test)


errors = abs(Y_pred - Y_test)
# Calculate mean absolute percentage error (MAPE)
mape = 100 * (errors / Y_test)
# Calculate and display accuracy
accuracy = 100 - np.mean(mape)
print('Accuracy:', round(accuracy, 2), '%.')


# %%
# Get numerical feature importances
importances = list(rf1.feature_importances_)
# List of tuples with variable and importance
feature_importances = [(feature, round(importance, 2)) for feature, importance in zip(feature_list, importances)]
# Sort the feature importances by most important first
feature_importances = sorted(feature_importances, key = lambda x: x[1], reverse = True)
# Print out the feature and importances 
[print('Variable: {:20} Importance: {}'.format(*pair)) for pair in feature_importances]

# %%



