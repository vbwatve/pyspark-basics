from common import read_csv
file_path = "h1b_data.csv"
df2 = read_csv(file_path)
df_selected = df2.select([
    'case_year', 'case_status', 'emp_name', 'emp_state', 'job_title', 'pw_level', 'work_city', 'lat', 'lng',
    "prevailing_wage"
])

# df_selected.show()



"""
df Filter 

-comparison filter
-isin List based filter
-notisin list based filter
-Multiple condition filter
-startwith, endswith, contains filter
-like, rlike pattern
"""

df_selected.\
    filter(df_selected.case_year < 2016).\
    show()

df_selected.\
    filter(df_selected.pw_level.isin(['Level II', 'Level IV'])).\
    show()

df_selected.\
    filter(~df_selected.pw_level.isin(['Level II', 'Level IV'])).\
    show()

df_selected.\
    filter((df_selected.case_year < 2016) & (df_selected.case_status == "CW")).\
    show()

df_selected.\
    filter((df_selected.emp_name.endswith("LLC") &
            (df_selected.emp_name.startswith("HEALTH")) &
            (df_selected.job_title.contains("THERAPIST")))).\
    show()

# use of 'like' similar to startswith
# like case-sensitive
df_selected.\
    filter(df_selected.job_title.like("THERA%")).\
    show()

# ilike case-insensitive
df_selected.\
    filter(df_selected.job_title.ilike("thera%")).\
    show()







