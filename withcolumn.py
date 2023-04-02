"""
Demonstration of withColumn API of learn_pyspark
"""
from pyspark.sql import functions
from common import read_csv

file_path = "h1b_data.csv"

df2 = read_csv(file_path)
df_selected = df2.select([
    'case_year', 'case_status', 'emp_name', 'emp_state', 'job_title', 'pw_level', 'work_city', 'lat', 'lng',
    "prevailing_wage"
])

"""
WithColumn to add extra column with when-otherwise function based on existing column values. 
"""
na_value = "Unknown"
df_selected.withColumn('cleaned_pw_level', functions.when(df2.pw_level == "N/A", na_value).otherwise(
    df2.pw_level)).\
    show()


"""
To add new column with constant value. 
"""
df_selected.withColumn('new_column_with_const', functions.lit('MY VALUE')).\
    show()




"""
WithColumn to cast the value of column.
Here column name is of existing column so value got replaced instead of adding new column
"""
df_selected.withColumn('prevailing_wage', functions.col('prevailing_wage').\
                       cast('Float')).show()


"""
IMP notes about lit(). 
- Used to provide constant values to columns
- with column always expects the type of Column class and lit returns an type of Column. 

"""
print(df_selected.withColumn('new_column_with_const', functions.lit(123)).dtypes)
print(df_selected.withColumn('new_column_with_const', functions.lit(123.20)).dtypes)
try:
    print(df_selected.withColumn('new_column_with_const', 123).dtypes)
except Exception as e:
    print(e)

try:
    print(df_selected.withColumn('new_column_with_const', "abc").dtypes)
except Exception as e:
    print(e)



