import json
import sys

import pyarrow.parquet as pq

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
except ValueError as e:
	print(f"Unable to decode ARK metadata: %e", e, file=sys.stderr)
	sys.exit(1)

keys = list(info.keys())
keys.sort()
maxlen = 0
for k in keys:
	if len(k) > maxlen:
		maxlen = len(k)

for k in keys:
	print(f'{k}{" " * (maxlen - len(k))}\t{info[k]}')
