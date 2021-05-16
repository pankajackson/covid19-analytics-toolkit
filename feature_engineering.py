import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Set Figure Size
plt.figure(figsize=(25, 25))



# Load DataSet
df = pd.read_csv('data/raw/owid-covid-data.csv')
print(df)


# set properties to print all column and rows
pd.set_option('display.max_column', None)
pd.set_option('display.max_row', None)


# Get Information of DataSet
df.info()


# Get All Null Values
# print(df.isnull()) # Will return True and False DataFrame
print((df.isnull().sum())) # Print Number of missing data present per column


# Check % of data Missing per column
# ((count of missing values)/(Total number of rows)) * 100
df_missing_percentage_per_column = df.isnull().sum()/df.shape[0] * 100
print(df_missing_percentage_per_column)


# Plot Heatmap to show Missing values
sns.heatmap(df.isnull())

plt.show()