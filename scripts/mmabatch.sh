#! /bin/bash

START=$(date -d "$1" +%Y-%m-%d)
END=$(date -d "$2" +%Y-%m-%d)
OUT=$3
IN=$4

while true; do
  FILE="$OUT/$(date -d "$START" +%Y/%j).csv.gz"
  mmaconv -z -r -w $FILE "$IN/$(date -d $START +%Y/%j)"
  if [[ $? -ne 0 ]]; then
    exit 1
  fi

  if [[ $START == $END ]]; then
    break;
  fi
  START=$(date -d "$START + 1 days" +%Y-%m-%d)
done
