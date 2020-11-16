import argparse
import pickle

import tracking_client
from tracking_client import Configuration, SampleApi


def download(isolate_id_list_filepath, tracking_api_host_port):
    [host, port] = tracking_api_host_port.split(':')

    configuration = Configuration()
    configuration.host = f"http://{host}:{port}/api/v1"
    api_client = tracking_client.ApiClient(configuration=configuration)

    sample_api = SampleApi(api_client)

    with open(isolate_id_list_filepath, 'r') as inf:
        mapping = {}

        for iid in inf:
            from_tracking_api = sample_api.samples_get(isolate_id=iid)

            if not from_tracking_api:
                mapping[iid] = iid
                continue

            mapping[iid] = from_tracking_api[0].id

        pickle.dump(mapping, open("{filepath}.pickle".format(filepath=isolate_id_list_filepath), "wb"))


parser = argparse.ArgumentParser(description='Download ID mapping from Tracking API.')
parser.add_argument('isolate_id_list_filepath', type=str, help='Path to input file. Expects each line is an isolate ID')
parser.add_argument('tracking_api_host_port', type=str, help='Tracking API host:port')

if __name__ == '__main__':
    args = parser.parse_args()
    download(args.isolate_id_list_filepath, args.tracking_api_host_port)