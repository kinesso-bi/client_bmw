import json
import time

import pandas as pd
import requests

import functions


def create_access_token(client_id, client_secret):
    response = requests.post("https://id.adform.com/sts/connect/token", data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.adform.com/scope/buyer.stats"
    })

    access_token = response.json()['access_token']
    return access_token


def read_access_token(scope):
    with open("credentials.json", "r") as jsonFile:
        data = json.load(jsonFile)

    access_token = data[scope]["token"]
    return access_token


def update_access_token(scope, access_token):
    with open("credentials.json", "r") as jsonFile:
        data = json.load(jsonFile)

    data[scope]["token"] = access_token

    with open("credentials.json.", "w") as jsonFile:
        json.dump(data, jsonFile)


def post_operation(client_id, client_secret, scope):
    access_token = read_access_token(scope)
    body = {
        "dimensions": ["date", "client", "campaign", "page"],
        "metrics": [
            {
                "metric": "impressions",
                "specs": {"adUniqueness": "campaignUnique"}
            },
            {
                "metric": "clicks",
                "specs": {"adUniqueness": "campaignUnique"}
            },
            {
                "metric": "sysvarProductCount"
            }
        ],
        "filter": {
            "date": "lastWeek"
        }
    }

    # "date": {
    #     "from": "2021-08-23",
    #     "to": "2021-08-29"}
    # "date": "lastWeek"

    response = requests.post(
        "https://api.adform.com/v1/buyer/stats/data",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(access_token)
        },
        json={
            'dimensions': body['dimensions'],
            'metrics': body['metrics'],
            'filter': body['filter']
        }
    )
    if response.status_code != 202:
        print("failed {}".format(response.status_code))
        print(response.text)
        time.sleep(10)
        access_token = create_access_token(client_id, client_secret)
        update_access_token(scope, access_token)
        print("updated json")
        post_operation(client_id, client_secret, scope)
    else:
        print("success {}".format(response.status_code))
        print(response.headers)
        time.sleep(5)
        print(response.headers)
        location = response.headers['Location']
        operation = response.headers['Operation-Location']
        read_operation_status(scope, operation)
        functions.upload_data(dataset_name="bmw", table_name="adform", input_data=read_location(scope, location))


def read_operation_status(scope, operation):
    access_token = read_access_token(scope)
    response = requests.get(
        "https://api.adform.com/{}".format(operation),
        headers={"Authorization": "Bearer {}".format(access_token)}
    )

    if 200 == response.status_code:
        pass
    else:
        time.sleep(10)
        print("read operation {}".format(response.status_code))
        read_operation_status(scope, operation)
    print(response.status_code)


def read_location(scope, location):
    access_token = read_access_token(scope)
    response = requests.get(
        "https://api.adform.com/{}".format(location),
        headers={"Authorization": "Bearer {}".format(access_token)}
    )
    if response.status_code != 200:
        with open("client_secrets.json", "r") as jsonFile:
            data = json.load(jsonFile)
        client_id = data["adform"][scope]["client_id"]
        client_secret = data["adform"][scope]["client_secret"]
        post_operation(client_id, client_secret, scope)

    else:
        print("read location {}".format(response.status_code))
        rows = response.json()['reportData']['rows']
        headers = response.json()['reportData']['columnHeaders']
        df = pd.DataFrame(rows)
        df.columns = headers
        df['date'] = df['date'].map(lambda x: str(x)[:10])
        return df


def setup(scope):
    with open("client_secrets.json", "r") as jsonFile:
        data = json.load(jsonFile)
    client_id = data["adform"][scope]["client_id"]
    client_secret = data["adform"][scope]["client_secret"]
    post_operation(client_id, client_secret, scope)


if __name__ == '__main__':
    setup(scope="bmw")
