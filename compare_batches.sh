#!/bin/sh

# Check if two directories are provided as arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 directory1 directory2"
    exit 1
fi

dir1=$1
dir2=$2

# Check if the directories exist
if [ ! -d "$dir1" ] || [ ! -d "$dir2" ]; then
    echo "Both arguments must be valid directories."
    exit 1
fi

# Iterate over files in the first directory
for left_file in "$dir1"/*; do
    # Determine the corresponding file in the second directory
    right_file="$dir2"/$(basename "$left_file")

    # Check if the corresponding file exists
    if [ -f "$right_file" ]; then
        echo "Comparing $(basename "$left_file") with its counterpart in $dir2..."
        # Call the comparison script
        python -m patex.compare_model_outputs --quiet "$left_file" "$right_file"
    else
        echo "No counterpart for $(basename "$left_file") found in $dir2."
        exit 1
    fi
done
