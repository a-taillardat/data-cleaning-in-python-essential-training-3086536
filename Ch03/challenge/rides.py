# %%
import pandas as pd
import numpy as np

df = pd.read_csv('rides.csv')
df
# %%
# Find out all the rows that have bad values
# - Missing values are not allowed
# - A plate must be a combination of at least 3 upper case letters or digits
# - Distance much be bigger than 0
# %%
df.dtypes
# %%
# Missing values

# verification for string values ''
df[['name', 'plate']] = df[['name', 'plate']]\
  .apply(lambda x: x.str.strip()).replace('', np.nan)

## or:
#df['name'] = df['name'].str.strip()
#df['plate'] = df['plate'].str.strip()
#df.replace('', np.nan)

df.dropna(inplace=True)
df

# %%
# Plates
df = df[(df['plate'].str.len() >= 3) &\
        (df['plate'] == df['plate'].str.upper())]
df
# %%
df = df[df['distance'] > 0]
df