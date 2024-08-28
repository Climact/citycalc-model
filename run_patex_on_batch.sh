#!/bin/sh

# Check if at least one argument is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 output_dir [max_jobs]"
    exit 1
fi

# Assign the first argument as the output directory
output_dir=$1

# Set max_jobs to the second argument or default to 1
max_jobs=${2:-1}

# Ensure the output directory exists
mkdir -p "$output_dir"

# Regions and lever values to use
regions=(BE BE2 CN CZ EL EU27 FR)
levers=(1 2 3 4)

# Base workspace and output directory
workspace="dev"

# Function to draw the progress bar
draw_progress_bar() {
    # $1 - total steps, $2 - current step, $3 - region, $4 - lever
    pct=$(( ($2 * 100) / $1 ))
    bar=""
    for ((i=0; i<50; i++)); do
        if [ $i -lt $((pct / 2)) ]; then
            bar="${bar}#"
        else
            bar="${bar}-"
        fi
    done
    printf "\r[%-50s] %d%% - Region: %s, Lever: %s" "$bar" "$pct" "$3" "$4"
}

# Total number of operations
total_operations=$((${#regions[@]} * ${#levers[@]}))
current_operation=0

run_patex() {
    region=$1
    lever=$2
    output="${output_dir}/${region}_${lever}.pkl"
    python -m patex --workspace "$workspace" --levers-default $lever -o "$output" "$region"
}

# Iterate over regions
for region in "${regions[@]}"; do
  # Iterate over lever values
  for lever in "${levers[@]}"; do
    current_operation=$((current_operation + 1))

    # Run patex in the background
    run_patex "$region" "$lever" &

    # Limit the number of parallel jobs
    if [[ $(jobs -r -p | wc -l) -ge $max_jobs ]]; then
        wait -n
    fi

    draw_progress_bar $total_operations $current_operation $region $lever
  done
done

wait

echo -e "\nProcessing complete."
