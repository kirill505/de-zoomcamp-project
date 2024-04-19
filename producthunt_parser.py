import datetime
import requests
import time
import json
import os
import argparse


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


def main(date1, date2, api_token_ph):
    while date1 > date2:
        ph = PH(api_token_ph)
        curr_date = date2 + datetime.timedelta(1)

        data = ph.get_posts_by_date(curr_date, date2)

        with open(f'./data/{date2.year}/ph_posts_{date2}.json', 'w') as f:
            json.dump(data, f)

        print(f"parsed data: {date2} - {len(data)}")

        date2 += datetime.timedelta(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date", type=str)
    parser.add_argument("-k", "--api_key", type=str)
    args = parser.parse_args()

    api_token_ph = os.environ[args.api_key]
    date1, date2 = args.date.split(" ")

    print(api_token_ph)

    main(
        datetime.datetime.strptime(date1, '%Y-%m-%d').date(),
        datetime.datetime.strptime(date2, '%Y-%m-%d').date(),
        api_token_ph
    )
