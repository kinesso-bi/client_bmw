"""Hello Analytics Reporting API V4."""

from datetime import datetime, timedelta

import httplib2
import pandas as pd
from apiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools

import functions

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
CLIENT_SECRETS_PATH = "oauth_client.json"
VIEW_ID = '179973098'
VIEW_ID = '184430432'


def initialize_analyticsreporting():
    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        CLIENT_SECRETS_PATH, scope=SCOPES,
        message=tools.message_if_missing(CLIENT_SECRETS_PATH))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage('analyticsreporting.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', http=http)

    return analytics


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

    return analytics.reports().batchGet(
        body={'reportRequests': [{'viewId': view_id,
                                  'dateRanges': dateRanges,
                                  'metrics': metrics,
                                  'dimensions': dimensions,
                                  }]}
    ).execute()


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


def main():
    views = ['179973098', '184430432']
    now = datetime.now()
    date_end = now - timedelta(days=now.weekday() + 1)
    date_start = date_end - timedelta(days=6)
    for view in views:
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, view_id=view, since=date_start, until=date_end)
        upload = get_response(response)
        if view == '179973098':
            functions.upload_data(dataset_name="bmw", table_name='ga_rzj', input_data=upload)
        elif view == '184430432':
            functions.upload_data(dataset_name="bmw", table_name='ga_kalkulator', input_data=upload)


if __name__ == '__main__':
    main()
