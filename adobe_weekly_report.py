import pandas as pd

campaign = pd.read_csv("adobe_campaign.csv")
organic = pd.read_csv("adobe_organic.csv")
sources = pd.read_csv("adobe_sources.csv")
reports = [campaign,organic,sources]
import functions
tables = ['adobe_campaign_traffic', 'adobe_organic_traffic', 'new_adobe_sources']

for report, table in zip(reports,tables):
    functions.upload_data("bmw", table, report)
