"""
Usage:
python find_max_feed_id.py

"""
import argparse
import json
import random
import sys

import pandas as pd
import podcastindex
from tqdm import tqdm


def main(
        api_key: str = None,
        api_secret: str = None,
        num_tries=10_000):

    if api_key and api_secret:
        config = {
            "api_key": "YOUR API KEY HERE",
            "api_secret": "YOUR API SECRET HERE"
        }
    else:
        with open("keys.json", "r") as f:
            config = json.load(f)

    max_feed_id = -1
    index = podcastindex.init(config)
    for _ in range(num_tries):

        feed_id = random.randint(0, 100_000_000)
        feed = index.podcastByFeedId(feed_id).get('feed')

        if not feed:
            print(f'skipping empty feed id %s' % feed_id)
            continue
        else:
            max_feed_id = max(max_feed_id, feed_id)
            print(f'got valid feed %s; current max is %s' % (feed_id, max_feed_id))
    print(f'max feed_id is %s' % max_feed_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", type=str)
    parser.add_argument("--api-secret", type=str)
    args = parser.parse_args()
    main(**vars(args))
