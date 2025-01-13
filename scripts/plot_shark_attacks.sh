#! /usr/bin/env bash

# Define arrays for shark species and countries
shark_species=(
    "White shark"
    "Tiger shark"
    "Bull shark"
    "Hammerhead shark"
    "Mako shark"
)

countries=(
    "USA"
    "Australia"
    "South Africa"
    "Brazil"
    "Mexico"
    "Bahamas"
    "Reunion"
)

# Define the path to the Python script
script_path="src/plot_shark_attacks.py"

# Create the output directory if it doesn't exist
mkdir -p "plots"

echo "Processing all species globally"
python $script_path --outDir "plots"

# Loop through each combination of shark species and country
for country in "${countries[@]}"; do
    # Create a directory for each country
    mkdir -p "plots/$country"

    echo "Processing: all species in $country"
    if python $script_path --country "$country" --outDir "plots/$country" 2> /dev/null; then
        for species in "${shark_species[@]}"; do
            echo "Processing: $species in $country"
            if python $script_path --country "$country" --species "$species" --outDir "plots/$country" 2> /dev/null; then
                echo "Processing completed for $species in $country!"
            else
                echo "No data available for $species in $country."
            fi
        done

        echo "Processing completed for $country!"
    else
        echo "No data available in $country."
    fi
    echo "------------------------------------"
done

echo "All processing completed!"
echo "Figures have been saved in the 'plots' directory."
