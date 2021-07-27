#!/bin/bash

# Usage: bash prepare_flink_input.sh <input_file_path> <output_file_path>

sed -E 's/^[^,]*,//g' $1 | sed -E 's/(.{4})-(..)-(..) (..:..:..)/\4 \3\/\2\/\1/g' | sed -E 's/,(,|\n)/,-1.0\1/g' | sed -E 's/,(,|\n)/,-1.0\1/g' | sed -E 's/,$/,-1.0/g' | tee $2
