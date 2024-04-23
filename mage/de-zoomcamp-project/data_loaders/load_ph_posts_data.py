import io
import pandas as pd
import requests
import datetime
import time
import json
import os
from mage_ai.data_preparation.shared.secrets import get_secret_value


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def fetch_data(query, api_token_ph):
    API_URL = "https://api.producthunt.com/v2/api/graphql"

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + api_token_ph,
        'Host': 'api.producthunt.com'
    }
    today_posts = requests.post(API_URL, headers=headers, data=json.dumps(query))

    if today_posts.status_code!= 429:
        return today_posts.json()["data"]["posts"]["edges"], int(today_posts.headers["x-rate-limit-remaining"]), int(today_posts.headers[
        "x-rate-limit-reset"])
    else:
        return [], int(today_posts.headers["x-rate-limit-remaining"]), int(today_posts.headers[
        "x-rate-limit-reset"])


class PH:
    def __init__(self, api_key):
        self.api_key = api_key

    def get_posts_by_date(self, date1, date2):
        query = {"query":
                     f'''
                    query dailyPosts {{
                            posts(postedBefore: "{date1}", postedAfter: "{date2}") {{
                            edges {{
                                cursor
                                node {{
                                    commentsCount
                                    createdAt
                                    description
                                    id
                                    name
                                    slug
                                    tagline
                                    url
                                    userId
                                    votesCount
                                    website
                                    topics {{
                                        edges {{
                                            node {{
                                                id
                                                name
                                                createdAt
                                            }}
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                '''}
        data, rate_limit_remaining, rate_limit_reset = fetch_data(query, self.api_key)

        if len(data) == 0:
            print(f"sleep {rate_limit_reset + 5} sec")
            time.sleep(rate_limit_reset + 5)
            data, rate_limit_remaining, rate_limit_reset = fetch_data(query, self.api_key)
        else:
            cursor = data[-1]['cursor']

        res = []

        while cursor:
            query = {"query":
                         f'''                            
                            query dailyPosts {{
                            posts(postedBefore: "{date1}", postedAfter: "{date2}", after: "{cursor}") {{
                            edges {{
                                cursor
                                node {{
                                    commentsCount
                                    createdAt
                                    description
                                    id
                                    name
                                    slug
                                    tagline
                                    url
                                    userId
                                    votesCount
                                    website
                                    topics {{
                                        edges {{
                                            node {{
                                                id
                                                name
                                                createdAt
                                            }}
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                            '''}

            data, rate_limit_remaining, rate_limit_reset = fetch_data(query, self.api_key)

            if rate_limit_remaining > 0:

                if len(data) == 0:
                    break
                res += data
                cursor = data[-1]['cursor']
            else:
                print(f"sleep {rate_limit_reset+5} sec")
                time.sleep(rate_limit_reset+5)

        print(rate_limit_remaining, rate_limit_reset)
        return res


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    api_token_ph = get_secret_value('ph_api_key1')

    ph = PH(api_token_ph)

    execution_date = kwargs.get('execution_date')
    print(f'execution date is: {execution_date}')

    date_end = execution_date.date()
    date_start = date_end - datetime.timedelta(1)
    
    
    data = ph.get_posts_by_date(date_end, date_start)


    filepath = f'/home/src/de-zoomcamp-project/ph_data/ph_posts_{date_start}.json'

    with open(filepath, 'w') as f:
        json.dump(data, f)
    res = pd.read_json(filepath)
    print(res)

    # return pd.read_csv(data, sep=',')
    return res


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
