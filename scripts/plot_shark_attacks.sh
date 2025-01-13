#! /usr/bin/env bash

# Define arrays for shark species and countries
shark_species=(
    "White"
    "Tiger"
    "Bull"
    "Hammerhead"
    "Mako"
)

countries=(
    "USA"
    "Australia"
    "South Africa"
    "Brazil"
    "Mexico"
)

script_path="src/plot_shark_attacks.py"

mkdir -p "plots"

echo "Processing all species globally"
python $script_path --outDir "plots"

# Loop through each combination of shark species and country
for country in "${countries[@]}"; do
    mkdir -p "plots/$country"

    echo "Processing: all species in $country"
    python $script_path --country "$country" --outDir "plots/$country"

    for species in "${shark_species[@]}"; do
        echo "Processing: $species in $country"
        if python $script_path --country "$country" --species "$species" --outDir "plots/$country" 2> /dev/null; then
            echo "Processing completed for $species in $country!"
        else
            echo "No data available for $species in $country."
        fi
    done

    echo "Processing completed for $country!"
    echo "------------------------------------"
done

echo "All processing completed!"
echo "Figures have been saved in the 'plots' directory."
