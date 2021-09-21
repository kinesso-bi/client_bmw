import pandas as pd
from datetime import datetime, timedelta
import functions

leads = pd.read_csv("transcom_files/leads_validated.csv")
df = pd.read_excel(open('transcom_files/lead_validated.xlsx', 'rb'),
                   sheet_name='Follow-up Source Data', skiprows=7, usecols='B:AD')

df['Created On'] = df['Created On'].dt.date
df['Modified On'] = df['Modified On'].dt.date

now = datetime.now()
date_end = now - timedelta(days=now.weekday() + 1)
date_start = date_end - timedelta(days=6)

date_end = pd.to_datetime(date_end).date()
date_start = pd.to_datetime(date_start).date()

df = df[(df['Created On'] >= date_start) & (df['Created On'] <= date_end)]
df.sort_values(by=['Created On'], inplace=True, ascending=True)
df.columns = leads.columns

reports = [df]
tables = ['leads_validated']


def main() -> bool:
    for report, table in zip(reports, tables):
        functions.upload_data("bmw", table, report)
    return True
