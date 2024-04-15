import argparse
import json
from collections import defaultdict


def main(filename: str, param: str, outfile_template: str):
    with open(filename) as f_obj:
        data = json.load(f_obj)
    results = defaultdict(lambda: [])
    for benchmark in data:
        results[benchmark["params"].pop(param)].append(benchmark)

    for key, data in results.items():
        with open(outfile_template.format(**{param: key}), "w") as outfile_f:
            json.dump(data, outfile_f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", required=True)
    parser.add_argument("--param", required=True)
    parser.add_argument("--outfile-template", required=True)
    args = vars(parser.parse_args())

    main(**args)
