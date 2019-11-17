import pandas as pd
import sqlite3
import os

salaries_2018 = pd.read_csv("salaries_2018.csv", delimiter= ";",engine="python",
                            encoding="utf-8")
unemployment_rate_2018 = pd.read_csv("unemployment_rate_2018.csv", delimiter= ";",
                                     engine="python", encoding="utf-8")
divorces_2018 = pd.read_csv("divorces_2018.csv", delimiter= ";",engine="python",
                            encoding="utf-8")
mortality_before_sixty_2018 = pd.read_csv("mortality_before_sixty_2018.csv",
                                          delimiter= ";",engine="python", encoding="utf-8")

for file in [salaries_2018, unemployment_rate_2018, divorces_2018, mortality_before_sixty_2018]:
    file.columns=["ID", "Region", "Value", "unnamed"]
    file["Value"] = file["Value"].replace(',','.', regex=True)
    file["Value"] = pd.to_numeric(file["Value"], errors='coerce')
    file.drop("unnamed", axis=1, inplace=True)

salaries_2018.columns.values[2] = "Salary_2018"
unemployment_rate_2018.columns.values[2] = "Unemployment_rate_2018"
divorces_2018.columns.values[2] = "Divorces_2018"
mortality_before_sixty_2018.columns.values[2] = "Mortality_before_sixty_2018"

db_name = 'best_county_2018.db'
con = sqlite3.connect(os.path.join(os.getcwd(), db_name))

salaries_2018.to_sql("salaries_2018", con, if_exists='replace', index=False)
unemployment_rate_2018.to_sql("unemployment_rate_2018", con, if_exists='replace',
                              index=False)
divorces_2018.to_sql("divorces_2018", con, if_exists='replace', index=False)
mortality_before_sixty_2018.to_sql("mortality_before_sixty_2018", con, if_exists='replace',
                                   index=False)

sql_result = pd.read_sql_query("""
SELECT 
s.Region,
s.Salary_2018,
u.Unemployment_rate_2018,
d.Divorces_2018,
m.Mortality_before_sixty_2018
FROM salaries_2018 AS s
JOIN unemployment_rate_2018 AS u
ON s.ID = u.ID
JOIN divorces_2018 AS d
ON d.ID = u.ID
JOIN mortality_before_sixty_2018 as m
ON m.ID = u.ID
WHERE s.ID NOT IN (0, 200000, 400000, 600000, 800000,1000000, 
1200000, 1400000,1600000, 1800000, 2000000,2200000,2400000,
2600000,2800000,3000000,3200000)
AND Salary_2018 > (SELECT AVG(Salary_2018) FROM salaries_2018)
AND Unemployment_rate_2018 < (SELECT AVG(Unemployment_rate_2018) 
FROM unemployment_rate_2018)
AND Divorces_2018 < (SELECT AVG(Divorces_2018) FROM divorces_2018)
AND Mortality_before_sixty_2018 < (SELECT AVG(Mortality_before_sixty_2018) 
FROM mortality_before_sixty_2018)
ORDER BY s.Salary_2018 DESC


""", con)
print(sql_result)
