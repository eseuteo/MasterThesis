#!/bin/bash

# Usage: bash prepare_flink_input.sh <input_folder> <output_folder>

for file in $(ls $1); do
    bash reformat_flink_input.sh $1/$file $2/$file
done
