import pandas as pd
import sqlite3
import os

salaries_2018 = pd.read_csv("salaries_2018.csv", delimiter= ";",engine="python",
                            encoding="utf-8")
salaries_2017 = pd.read_csv("salaries_2017.csv", delimiter= ";",engine="python",
                            encoding="utf-8")
salaries_2016 = pd.read_csv("salaries_2016.csv", delimiter= ";",engine="python",
                            encoding="utf-8")
unemployment_rate_2018 = pd.read_csv("unemployment_rate_2018.csv", delimiter= ";",
                                     engine="python", encoding="utf-8")
unemployment_rate_2017 = pd.read_csv("unemployment_rate_2017.csv", delimiter= ";",
                                     engine="python", encoding="utf-8")
unemployment_rate_2016 = pd.read_csv("unemployment_rate_2016.csv", delimiter= ";",
                                     engine="python", encoding="utf-8")
divorces_2018 = pd.read_csv("divorces_2018.csv", delimiter= ";",engine="python",
                            encoding="utf-8")
divorces_2017 = pd.read_csv("divorces_2017.csv", delimiter= ";",engine="python",
                            encoding="utf-8")
divorces_2016 = pd.read_csv("divorces_2016.csv", delimiter= ";",engine="python",
                            encoding="utf-8")
mortality_before_sixty_2018 = pd.read_csv("mortality_before_sixty_2018.csv",
                                          delimiter= ";",engine="python", encoding="utf-8")
mortality_before_sixty_2017 = pd.read_csv("mortality_before_sixty_2017.csv",
                                          delimiter= ";",engine="python", encoding="utf-8")
mortality_before_sixty_2016 = pd.read_csv("mortality_before_sixty_2016.csv",
                                          delimiter= ";",engine="python", encoding="utf-8")

salaries_2018.columns=["ID", "Region", "Salary_2018","unnamed"]
salaries_2018.drop("unnamed", axis=1, inplace=True)
salaries_2018["Salary_2018"] = salaries_2018["Salary_2018"].replace(',','.', regex=True)
salaries_2018["Salary_2018"] = pd.to_numeric(salaries_2018["Salary_2018"], errors='coerce')

salaries_2017.columns=["ID", "Region", "Salary_2017","unnamed"]
salaries_2017.drop("unnamed", axis=1, inplace=True)
salaries_2017["Salary_2017"] = salaries_2017["Salary_2017"].replace(',','.', regex=True)
salaries_2017["Salary_2017"] = pd.to_numeric(salaries_2017["Salary_2017"], errors='coerce')

salaries_2016.columns=["ID", "Region", "Salary_2016","unnamed"]
salaries_2016.drop("unnamed", axis=1, inplace=True)
salaries_2016["Salary_2016"] = salaries_2016["Salary_2016"].replace(',','.', regex=True)
salaries_2016["Salary_2016"] = pd.to_numeric(salaries_2016["Salary_2016"], errors='coerce')

unemployment_rate_2018.columns=["ID", "Region", "Unemployment_rate_2018","unnamed"]
unemployment_rate_2018.drop("unnamed", axis=1, inplace=True)
unemployment_rate_2018["Unemployment_rate_2018"] = unemployment_rate_2018\
    ["Unemployment_rate_2018"].replace(',','.', regex=True)
unemployment_rate_2018["Unemployment_rate_2018"] = pd.to_numeric\
    (unemployment_rate_2018["Unemployment_rate_2018"], errors='coerce')

unemployment_rate_2017.columns=["ID", "Region", "Unemployment_rate_2017","unnamed"]
unemployment_rate_2017.drop("unnamed", axis=1, inplace=True)
unemployment_rate_2017["Unemployment_rate_2017"] = unemployment_rate_2017\
    ["Unemployment_rate_2017"].replace(',','.', regex=True)
unemployment_rate_2017["Unemployment_rate_2017"] = pd.to_numeric\
    (unemployment_rate_2017["Unemployment_rate_2017"], errors='coerce')

unemployment_rate_2016.columns=["ID", "Region", "Unemployment_rate_2016","unnamed"]
unemployment_rate_2016.drop("unnamed", axis=1, inplace=True)
unemployment_rate_2016["Unemployment_rate_2016"] = unemployment_rate_2016\
    ["Unemployment_rate_2016"].replace(',','.', regex=True)
unemployment_rate_2016["Unemployment_rate_2016"] = pd.to_numeric\
    (unemployment_rate_2016["Unemployment_rate_2016"], errors='coerce')

divorces_2018.columns=["ID", "Region", "Divorces_2018","unnamed"]
divorces_2018.drop("unnamed", axis=1, inplace=True)
divorces_2018["Divorces_2018"] = divorces_2018["Divorces_2018"].replace(',','.', regex=True)
divorces_2018["Divorces_2018"] = pd.to_numeric(divorces_2018["Divorces_2018"], errors='coerce')

divorces_2017.columns=["ID", "Region", "Divorces_2017","unnamed"]
divorces_2017.drop("unnamed", axis=1, inplace=True)
divorces_2017["Divorces_2017"] = divorces_2017["Divorces_2017"].replace(',','.', regex=True)
divorces_2017["Divorces_2017"] = pd.to_numeric(divorces_2017["Divorces_2017"], errors='coerce')

divorces_2016.columns=["ID", "Region", "Divorces_2016","unnamed"]
divorces_2016.drop("unnamed", axis=1, inplace=True)
divorces_2016["Divorces_2016"] = divorces_2016["Divorces_2016"].replace(',','.', regex=True)
divorces_2016["Divorces_2016"] = pd.to_numeric(divorces_2016["Divorces_2016"], errors='coerce')

mortality_before_sixty_2018.columns=["ID", "Region", "Mortality_before_sixty_2018","unnamed"]
mortality_before_sixty_2018.drop("unnamed", axis=1, inplace=True)
mortality_before_sixty_2018["Mortality_before_sixty_2018"] = mortality_before_sixty_2018\
    ["Mortality_before_sixty_2018"].replace(',','.', regex=True)
mortality_before_sixty_2018["Mortality_before_sixty_2018"] = pd.to_numeric\
    (mortality_before_sixty_2018["Mortality_before_sixty_2018"], errors='coerce')

mortality_before_sixty_2017.columns=["ID", "Region", "Mortality_before_sixty_2017","unnamed"]
mortality_before_sixty_2017.drop("unnamed", axis=1, inplace=True)
mortality_before_sixty_2017["Mortality_before_sixty_2017"] = mortality_before_sixty_2017\
    ["Mortality_before_sixty_2017"].replace(',','.', regex=True)
mortality_before_sixty_2017["Mortality_before_sixty_2017"] = pd.to_numeric\
    (mortality_before_sixty_2017["Mortality_before_sixty_2017"], errors='coerce')

mortality_before_sixty_2016.columns=["ID", "Region", "Mortality_before_sixty_2016","unnamed"]
mortality_before_sixty_2016.drop("unnamed", axis=1, inplace=True)
mortality_before_sixty_2016["Mortality_before_sixty_2016"] = mortality_before_sixty_2016\
    ["Mortality_before_sixty_2016"].replace(',','.', regex=True)
mortality_before_sixty_2016["Mortality_before_sixty_2016"] = pd.to_numeric\
    (mortality_before_sixty_2016["Mortality_before_sixty_2016"], errors='coerce')

db_name = 'best_county_all.db'
con = sqlite3.connect(os.path.join(os.getcwd(), db_name))

salaries_2018.to_sql("salaries_2018", con, if_exists='replace', index=False)
salaries_2017.to_sql("salaries_2017", con, if_exists='replace', index=False)
salaries_2016.to_sql("salaries_2016", con, if_exists='replace', index=False)
unemployment_rate_2018.to_sql("unemployment_rate_2018", con, if_exists='replace',
                              index=False)
unemployment_rate_2017.to_sql("unemployment_rate_2017", con, if_exists='replace',
                              index=False)
unemployment_rate_2016.to_sql("unemployment_rate_2016", con, if_exists='replace',
                              index=False)
divorces_2018.to_sql("divorces_2018", con, if_exists='replace', index=False)
divorces_2017.to_sql("divorces_2017", con, if_exists='replace', index=False)
divorces_2016.to_sql("divorces_2016", con, if_exists='replace', index=False)
mortality_before_sixty_2018.to_sql("mortality_before_sixty_2018", con, if_exists='replace',
                                   index=False)
mortality_before_sixty_2017.to_sql("mortality_before_sixty_2017", con, if_exists='replace',
                                   index=False)
mortality_before_sixty_2016.to_sql("mortality_before_sixty_2016", con, if_exists='replace',
                                   index=False)

sql_result = pd.read_sql_query("""
SELECT salaries_2017.Region, 
salaries_2016.Salary_2016, 
salaries_2017.Salary_2017, 
salaries_2018.Salary_2018,
unemployment_rate_2016.Unemployment_rate_2016,
unemployment_rate_2017.Unemployment_rate_2017,
unemployment_rate_2018.Unemployment_rate_2018,
divorces_2016.Divorces_2016,
divorces_2017.Divorces_2017,
divorces_2018.Divorces_2018,
mortality_before_sixty_2016.Mortality_before_sixty_2016,
mortality_before_sixty_2017.Mortality_before_sixty_2017,
mortality_before_sixty_2018.Mortality_before_sixty_2018
FROM salaries_2017
JOIN salaries_2018
ON salaries_2017.ID = salaries_2018.ID
JOIN salaries_2016
ON salaries_2016.ID = salaries_2017.ID
JOIN unemployment_rate_2018
ON unemployment_rate_2018.ID = salaries_2017.ID
JOIN unemployment_rate_2017
ON unemployment_rate_2017.ID = salaries_2017.ID
JOIN unemployment_rate_2016
ON unemployment_rate_2016.ID = salaries_2017.ID
JOIN divorces_2016
ON divorces_2016.ID = salaries_2017.ID
JOIN divorces_2017
ON divorces_2017.ID = salaries_2017.ID
JOIN divorces_2018
ON divorces_2018.ID = salaries_2017.ID
JOIN mortality_before_sixty_2016
ON mortality_before_sixty_2016.ID = salaries_2017.ID
JOIN mortality_before_sixty_2017
ON mortality_before_sixty_2017.ID = salaries_2017.ID
JOIN mortality_before_sixty_2018
ON mortality_before_sixty_2018.ID = salaries_2017.ID
LIMIT 10

""", con)
print(sql_result)
