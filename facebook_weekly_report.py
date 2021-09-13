import json
import time
from datetime import datetime, timedelta

import pandas as pd

import functions


def get_insights(campaigns, since: datetime, until: datetime):
    try:
        with open('client_secrets.json') as file:
            credentials = json.load(file)
    except FileNotFoundError as f:
        # functions.error_log(1, 1, f.args[0], f.args[1])
        quit()

    params = {
        'api_token': credentials['facebook']['api_token'],
        'api_version': 'v11.0',
        'limit': '1000000',
        'fields': 'campaign_name,spend,impressions,clicks,reach,actions',
        'time_range': {"since": since.strftime("%Y-%m-%d"), "until": until.strftime("%Y-%m-%d")},
        'time_increment': '1'
    }

    for campaign in campaigns:
        time.sleep(3)
        url = 'https://graph.facebook.com/{API_VERSION}/{CAMPAIGN_ID}/insights?action_attribution_windows=default&fields={FIELDS}&limit={LIMIT}&time_range={TIME_RANGE}&time_increment=1&access_token={API_TOKEN}'.format(
            API_VERSION=params['api_version'], CAMPAIGN_ID=campaign, API_TOKEN=params['api_token'],
            FIELDS=params['fields'],
            LIMIT=params['limit'], TIME_RANGE=params['time_range']
        )

        data = functions.get_stream(url, campaign, 1)
        try:
            data = data.json()
            if data['data']:
                print(data['paging'])
                data = data['data']
                data_list = []
                dates_list = []
                for i, row in enumerate(data):
                    try:
                        data_list.extend(row['actions'])
                        dates = len(row['actions']) * [row['date_start']]
                        dates_list.extend(dates)
                    except:
                        pass

                df_data = pd.DataFrame(data_list)
                df_dates = pd.DataFrame(dates_list, columns=['date'])
                result = pd.concat([df_data, df_dates], axis=1).reindex(df_dates.index)
                # TODO check if there is 'next' with another data page
                functions.upload_data(dataset_name="bmw", table_name="facebook_test", input_data=result)
            else:
                pass
        except:
            pass


def main() -> object:
    with open('client_secrets.json') as file:
        credentials = json.load(file)

    now = datetime.now()
    date_end = now - timedelta(days=now.weekday() + 1)
    date_start = date_end - timedelta(days=6)

    get_insights(campaigns=credentials['facebook']['bmw']['ids'], since=date_start, until=date_end)
