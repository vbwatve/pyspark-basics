"""
Demonstration of groupby API of pyspark
"""

from pyspark.sql import functions
from common import read_csv

file_path = "h1b_data.csv"

df2 = read_csv(file_path)
df_selected = df2.select([
    'case_year', 'case_status', 'emp_name', 'emp_state', 'job_title', 'pw_level', 'work_city', 'lat', 'lng',
    "prevailing_wage", "emp_country", "emp_state"
]).withColumn('prevailing_wage', functions.col('prevailing_wage').cast('Float'))


"""
Grouping by case status to find out number of records of each status.
"""
df_selected.groupBy('case_status').count().show()


"""
Calculate the minimum prevailing wage of each job title using min().
similarly we can use min / max / mean/ avg
"""

df_selected.groupBy('job_title').min('prevailing_wage').show(truncate=False)


"""
Grouping data by multiple columns.
"""

df_selected.groupBy('emp_state', 'emp_country').min('prevailing_wage').show()


"""
Group by with multiple aggregations
"""
df_selected.groupBy('emp_state', 'emp_country').agg(functions.sum('prevailing_wage').alias('sum_prevailing_wage'),
                                                    functions.min('prevailing_wage').alias('min_prevailing_wage')).show()