from mage_ai.io.file import FileIO
from mage_ai.data_preparation.shared.secrets import get_secret_value
import datetime
import requests
import time
import json
import os
import argparse

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
def load_data_from_file(*args, **kwargs):
    """
    Template for loading data from filesystem.
    Load data from 1 file or multiple file directories.

    For multiple directories, use the following:
        FileIO().load(file_directories=['dir_1', 'dir_2'])

    Docs: https://docs.mage.ai/design/data-loading#fileio
    """

    api_token_ph = get_secret_value('ph_api_key2')
    print(api_token_ph)

    ph = PH(api_token_ph)

    print(kwargs.get('interval_start_datetime'),kwargs.get('interval_end_datetime'))

    # date_end = datetime.date.today()
    # date_start = date_end - datetime.timedelta(1)
    # print(date_start, date_end)
    
    # data = ph.get_posts_by_date(date_end, date_start)
    filepath = f'/home/src/default_repo/ph_data/ph_data/ph_posts_2024_04_17.json'

    # filepath = f'/home/src/default_repo/ph_data/ph_posts_{date_start}.json'

    # with open(filepath, 'w') as f:
    #     json.dump(data, f)

    # print(f"parsed data: {date_start} - {len(data)}")

    return FileIO().load(filepath)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
