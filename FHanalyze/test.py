from FHanalyze.analyze import Analyze

a = Analyze()
date_with_readings = "2020-01-13"
df = a.get_DataFrame_for_date(date_with_readings)
print(df.describe())
