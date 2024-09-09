import json
import sys
import time

import pyarrow.parquet as pq

def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]}", file=sys.stderr)
        sys.exit(1)

    file = pq.read_table(sys.argv[1])
    metadata = file.schema.metadata

    try:
        arkmetadata = metadata[b"experiment"]
    except KeyError:
        print("No ARK metadata on this file", file=sys.stderr)
        sys.exit(1)

    try:
        info = json.loads(arkmetadata)
    except ValueError as exc:
        print("Unable to decode ARK metadata: %e", exc, file=sys.stderr)
        sys.exit(1)

    keys = list(info.keys())
    keys.sort()
    maxlen = 0
    for k in keys:
        if len(k) > maxlen:
            maxlen = len(k)

    for k in keys:
        if k == 'timestamp':
            val = time.ctime(info[k])
        else:
            val = info[k]

        print(f'{k}{" " * (maxlen - len(k))}\t{val}')

if __name__ == "__main__":
    main()
