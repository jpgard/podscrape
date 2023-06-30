"""
Usage:
python get_urls.py --api-key YOUR_API_KEY --api-secret YOUR_API_SECRET

"""
import argparse
import datetime
import json

import pandas as pd
import podcastindex
from tqdm import tqdm


def main(
        max_episodes: int = 50_000_000,
        max_feed_id: int = 7_000_000,  # max ID was 6442332 as of 6/30/23
        api_key: str = None,
        api_secret: str = None,
        max_episodes_per_feed=10_000,
        outfile="episodes.csv"):
    episode_data = []
    if api_key and api_secret:
        config = {
            "api_key": "YOUR API KEY HERE",
            "api_secret": "YOUR API SECRET HERE"
        }
    else:
        with open("keys.json", "r") as f:
            config = json.load(f)

    index = podcastindex.init(config)
    timestamp = datetime.datetime.now()
    # while len(episode_data) < max_episodes:
    for feed_id in range(1, max_feed_id):
        print(f"processing feed %s" % feed_id)
        feed = index.podcastByFeedId(feed_id).get('feed')

        if not feed:
            print(f'skipping empty feed id %s' % feed_id)
            continue

        recent_episodes = index.episodesByFeedId(
            feed_id,
            max_results=max_episodes_per_feed,
            # since=-525600 # in the last year
        )
        if not recent_episodes['items']:
            continue
        else:
            print(f"got {len(recent_episodes['items'])} recent episodes"
                  f"\ncurrent total: {len(episode_data)}")
        for episode in tqdm(recent_episodes['items']):
            # results = index.episodeById(1270106072)
            url = episode['enclosureUrl']
            episode_data.append(
                dict(episode_url=url,
                     episode_title=episode['title'],
                     episode_date_published_pretty=episode['datePublishedPretty'],
                     episode_date_published=episode['datePublished'],
                     duration=episode['duration'],
                     is_explicit=episode['explicit'],
                     transcript_url=episode['transcriptUrl'],
                     enclosureLength=episode['enclosureLength'],
                     enclosureType=episode['enclosureType'],
                     enclosureUrl=episode['enclosureUrl'],
                     episodeType=episode['episodeType'],
                     feed_id=feed_id,
                     feed_title=feed['title'],
                     feed_categories=feed['categories'],
                     feed_explicit=feed['explicit'],
                     feed_language=feed['language'],
                     ))

        if (datetime.datetime.now() - timestamp).total_seconds() > (15 * 60):
            timestamp = datetime.datetime.now()
            print(f"[INFO] writing {len(episode_data)} episodes to {outfile}")
            pd.DataFrame(episode_data).to_csv(outfile, index=False)

    print(f"[INFO] writing {len(episode_data)} episodes to {outfile}")
    pd.DataFrame(episode_data).to_csv(outfile, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", type=str)
    parser.add_argument("--api-secret", type=str)
    parser.add_argument("--max-episodes", type=int, default=1000)
    args = parser.parse_args()
    main(**vars(args))
