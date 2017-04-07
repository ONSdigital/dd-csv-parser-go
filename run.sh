#!/usr/bin/env bash

BUCKET="dp-csv-splitter"
FILE="SNPP_2012_WARDH_2_EN.csv"

go build -o lib/spike
./lib/spike --bucket="$BUCKET" --file="$FILE"