import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
import json
import os
import argparse

def plot_shark_attacks(sst_data, rainfall_data, attack_dates):
    fig, ax = plt.subplots(figsize=(10, 8))

    attack_temps = sst_data[attack_dates]
    attack_rainfall = rainfall_data[attack_dates]

    temp_min, temp_max = sst_data.min(), sst_data.max()
    rain_min, rain_max = rainfall_data.min(), rainfall_data.max()

    xgrid = np.linspace(temp_min - 5, temp_max + 5, 400)
    ygrid = np.linspace(rain_min, rain_max + 0.1, 400)
    X, Y = np.meshgrid(xgrid, ygrid)

    positions = np.vstack([X.ravel(), Y.ravel()])
    values = np.vstack([attack_temps, attack_rainfall])
    kernel = gaussian_kde(values)
    Z = np.reshape(kernel(positions), X.shape)

    ax.set_xlim(temp_min - 5, temp_max + 5)
    ax.set_ylim(rain_min, rain_max + 0.1)
    pcm = ax.pcolormesh(X, Y, Z,
                        cmap='RdYlGn_r',
                        shading='auto')

    ax.plot(attack_temps, attack_rainfall,
        linewidth=0.5,
        marker='x',
        color='black',
        alpha=0.5,
        linestyle='none',
        label='Shark Attacks')

    ax.set_xlabel('Sea Surface Temperature (°C)')
    ax.set_ylabel('Rainfall (mm)')
    ax.set_title('Shark Attack Risk Distribution')

    cbar = plt.colorbar(pcm)
    cbar.set_label('Attack Probability Density')

    plt.legend()
    plt.tight_layout()
    return fig

def get_data(country: str, species: str):
    with open('out/shark_incidents.json') as data:
        df = pd.json_normalize(json.load(data))

    if country is not None:
        df = df[df['country'].apply(lambda c: c.lower() == country.lower())]

    if species is not None:
        df = df[df['shark_species'].apply(lambda s: (s is not None) and (species.lower() in s.lower()))]

    if (df.empty):
        raise "DataFrame is empty. Check your country and species inputs."

    # Create DateTimeIndex for attacks
    dates = pd.DatetimeIndex(pd.to_datetime(df['utc_datetime']).dt.floor('D'))

    # Create Series with DateTimeIndex for temperature and rainfall
    sst_data = pd.Series(df['weather.temperature_2m'].values, index=dates)
    rainfall_data = pd.Series(df['weather.rain'].values, index=dates)

    # Convert dates to numpy array of datetime64
    attack_dates = dates.to_numpy()
    return attack_dates, sst_data, rainfall_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot shark attack risk distribution.')
    parser.add_argument('--country', type=str, help='Filter data by country')
    parser.add_argument('--species', type=str, help='Filter data by shark species')
    parser.add_argument('--outDir', type=str, help='Output directory', default='.')
    args = parser.parse_args()

    country = args.country
    species = args.species

    attack_dates, sst_data, rainfall_data = get_data(country, species)

    fig = plot_shark_attacks(sst_data, rainfall_data, attack_dates)

    country = country.replace(' ', '_') if country else 'all'
    species = species.replace(' ', '_') if species else 'all'

    directory = args.outDir
    # If no country or species is specified, save as 'shark_attacks_global.png'
    if (not country and not species):
        fig.savefig(f'{directory}/shark_attacks_global.png')
    else:
        fig.savefig(f'{directory}/shark_attacks_{country}_{species}.png')

    print("Done!")
