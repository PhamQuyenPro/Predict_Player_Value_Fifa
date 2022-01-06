# IMPORT LIBRARIES
import pandas as pd 
import numpy as np 

# READ CSV FILE
df = pd.read_csv('Player_List_Crawl.csv', index_col=None)

# DROP DUPLICATE DATA
df_1 = df.drop_duplicates()

# DROP NAN DATA
df_2 = df_1.dropna(axis=0)

# CONVERT FEATURES INT64 TO FLOAT64
df_2[['Overall_Score', 'Potential_Score', 
	'Weekly_Salary', 'Height', 'Weight', 'Age']] = df_2[['Overall_Score', 'Potential_Score', 
														'Weekly_Salary', 'Height', 'Weight', 'Age']].astype(float)

# PRINT DATAFRAME INFO
print(df_2.info())

# SAVE CLEANED DATA FIFA TO CSV FILE
df_2.to_csv('Player_List_Cleaned_Data.csv', index=None)