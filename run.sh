#!/usr/bin/env bash

# Set as path to file you want to pass in.
INPUT_FILE=""

go build -o lib/spike
./lib/spike --in="$INPUT_FILE"