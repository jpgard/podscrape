"""
Usage:
python get_urls.py --resume-from-outfile

"""
import argparse
import datetime
import json

import pandas as pd
import podcastindex
from requests.exceptions import ConnectionError
from tqdm import tqdm
from urllib3.exceptions import ProtocolError


def main(
        max_episodes: int = 50_000_000,
        max_feed_id: int = 7_000_000,  # max ID was 6442332 as of 6/30/23
        api_key: str = None,
        api_secret: str = None,
        max_episodes_per_feed=10_000,
        resume_from_outfile: bool = False,
        outfile="episodes.csv"):
    current_data = []
    if api_key and api_secret:
        config = {
            "api_key": "YOUR API KEY HERE",
            "api_secret": "YOUR API SECRET HERE"
        }
    else:
        with open("keys.json", "r") as f:
            config = json.load(f)

    if resume_from_outfile:
        archived_data = pd.read_csv(outfile)
        print(f"[INFO] successfully read in {len(archived_data)} records")
        feed_id = archived_data['feed_id'].max()
    else:
        archived_data = None
        feed_id = 1

    index = podcastindex.init(config)
    timestamp = datetime.datetime.now()
    num_retries = 0
    max_num_retries = 2048
    while feed_id < max_feed_id:
        try:
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
                      f"\ncurrent total: {len(current_data)}")
            for episode in tqdm(recent_episodes['items']):
                # results = index.episodeById(1270106072)
                url = episode['enclosureUrl']
                current_data.append(
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
                archived_data = pd.concat((archived_data, pd.DataFrame(current_data)))
                print(f"[INFO] writing {len(current_data)} new episodes to {outfile}; {len(archived_data)} total")
                archived_data.to_csv(outfile, index=False)
                current_data = []
            if (archived_data is not None) and (len(archived_data) > max_episodes):
                print(f"[INFO] terminating with {len(archived_data)} episodes.")
                break
        except (ConnectionResetError, ConnectionError, ProtocolError) as e:
            print(f"[WARNING] got exception {e}; resetting connection")
            num_retries += 1
            if num_retries > max_num_retries:
                print(f"[WARNING] reached max num retries = {max_num_retries}; terminating")
                break
            else:
                index = podcastindex.init(config)

    archived_data = pd.concat((archived_data, pd.DataFrame(current_data)))
    print(f"[INFO] writing {len(current_data)} new episodes to {outfile}; {len(archived_data)} total")
    archived_data.to_csv(outfile, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", type=str)
    parser.add_argument("--api-secret", type=str)
    parser.add_argument("--max-episodes", type=int, default=1000)
    parser.add_argument("--resume-from-outfile",
                        default=False, action="store_true")
    args = parser.parse_args()
    main(**vars(args))
