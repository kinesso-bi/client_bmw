import pandas as pd

import functions

campaign = pd.read_csv("adobe_files/adobe_campaign.csv")
organic = pd.read_csv("adobe_files/adobe_organic.csv")
sources = pd.read_csv("adobe_files/adobe_sources.csv")
reports = [campaign, organic, sources]

tables = ['adobe_campaign_traffic', 'adobe_organic_traffic', 'new_adobe_sources']


def main() -> object:
    for report, table in zip(reports, tables):
        functions.upload_data("bmw", table, report)
    return True
