#! /bin/bash

for file in $(ls $1); do
    cat $1/$file | tail -n +2 >> $2
done