#!/bin/bash

cd "$HOME/Downloads/CollegeScorecard_Raw_Data_08032021"

for filename in *
do
  # this syntax emits the value in lowercase: ${var,,*}  (bash version 4)
  case  "${filename}" in 
    MERGED*.csv)   mv "$filename" "$HOME/Desktop/ML_PATH/week0/collegeScoreCard/data/" ;;
    # Most*.csv)     mv "$filename" "$HOME/Desktop/ML_PATH/week0/collegeScoreCard/data/other_data/" ;;
    *)   echo "don't know where to put $filename"
  esac
done
