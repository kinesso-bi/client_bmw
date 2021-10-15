"""Hello Analytics Reporting API V4."""

from datetime import datetime, timedelta
import os
import httplib2
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import functions

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
path = os.path.dirname(os.path.realpath(__file__))
CLIENT_SECRETS_PATH = '{}/service_account.json'.format(path)


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRETS_PATH, SCOPES)
    # Create a service object
    http = credentials.authorize(httplib2.Http())
    service = build('analytics', 'v4', http=http,
                    discoveryServiceUrl=('https://analyticsreporting.googleapis.com/$discovery/rest'))
    return service


def get_report(analytics, view_id: str, since: datetime, until: datetime):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    dateRanges = [{'startDate': since.strftime("%Y-%m-%d"), 'endDate': until.strftime("%Y-%m-%d")}]
    metrics = [{'expression': 'ga:totalEvents'}, ]
    dimensions = [{'name': 'ga:date'},
                  {'name': 'ga:eventCategory'},
                  {'name': 'ga:eventAction'},
                  {'name': 'ga:campaign'},
                  {'name': 'ga:sourceMedium'},
                  ]

    response = analytics.reports().batchGet(
        body={'reportRequests': [{'viewId': view_id,
                                  'dateRanges': dateRanges,
                                  'metrics': metrics,
                                  'dimensions': dimensions,
                                  }]}
    ).execute()
    return response


def get_response(response):
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])
        dimensionHeaders.extend([metricHeaders[0]['name']])
        df = pd.DataFrame(columns=dimensionHeaders)
        for row in rows:
            a = row['dimensions']
            b = row['metrics'][0]['values']
            a.extend(b)
            df = df.append(pd.Series(a, index=dimensionHeaders), ignore_index=True)
        df['ga:date'] = pd.to_datetime(df['ga:date'].astype(str), format='%Y%m%d')
        df['ga:date'] = df['ga:date'].dt.date
        df.columns = ['Date', 'Event_category', 'Event_action', 'Campaign', 'Source_medium', 'Events_count']
        return df


def main(date_start=None, date_end=None) -> object:
    views = ['179973098', '184430432']
    now = datetime.now()
    if not date_end or not date_start:
        date_end = now - timedelta(days=now.weekday() + 1)
        date_start = date_end - timedelta(days=6)
    print('function', date_start, date_end)
    for view in views:
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, view_id=view, since=date_start, until=date_end)
        upload = get_response(response)
        if view == '179973098':
            print(view)
            # functions.upload_data(dataset_name="bmw", table_name='ga_rzj', input_data=upload)
        elif view == '184430432':
            print(view)
            # functions.upload_data(dataset_name="bmw", table_name='ga_kalkulator', input_data=upload)
    return True


# date_start = datetime.strptime('2020-01-01', "%Y-%m-%d")
# date_end = datetime.strptime('2020-12-31', "%Y-%m-%d")
# main(date_end=date_end, date_start=date_start)
# main()

# start_date = datetime.strptime('2020-12-30', "%Y-%m-%d")
# end_date = datetime.strptime('2020-12-31', "%Y-%m-%d")
# delta = timedelta(days=7)
# while start_date <= end_date:
#     print(start_date, start_date + timedelta(days=6))
#     main(date_start=start_date, date_end=start_date + timedelta(days=6))
#     start_date += delta