import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from scipy.stats import gaussian_kde
import json
import argparse
from typing import Tuple
from pathlib import Path


def create_density_plot(
    attack_temps: np.ndarray,
    attack_rainfall: np.ndarray,
    temp_range: Tuple[float, float],
    rain_range: Tuple[float, float],
    resolution: int = 400,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    xgrid = np.linspace(temp_range[0] - 5, temp_range[1] + 5, resolution)
    ygrid = np.linspace(rain_range[0], rain_range[1] + 0.1, resolution)
    X, Y = np.meshgrid(xgrid, ygrid)

    positions = np.vstack([X.ravel(), Y.ravel()])
    values = np.vstack([attack_temps, attack_rainfall])
    kernel = gaussian_kde(values)
    Z = np.reshape(kernel(positions), X.shape)
    return X, Y, Z


def setup_plot_aesthetics(
    ax: plt.Axes,
    temp_range: Tuple[float, float],
    rain_range: Tuple[float, float],
    title: str,
) -> None:
    ax.set_xlim(temp_range[0] - 5, temp_range[1] + 5)
    ax.set_ylim(rain_range[0], rain_range[1] + 0.1)
    ax.set_xlabel("Sea Surface Temperature (°C)")
    ax.set_ylabel("Rainfall (mm)")
    ax.set_title(title)


def plot_shark_attacks(
    sst_data: pd.Series,
    rainfall_data: pd.Series,
    attack_dates: pd.DatetimeIndex,
    country: str,
    species: str,
) -> Figure:
    fig, ax = plt.subplots(figsize=(10, 8))

    attack_temps = sst_data[attack_dates]
    attack_rainfall = rainfall_data[attack_dates]

    temp_range = (sst_data.min(), sst_data.max())
    rain_range = (rainfall_data.min(), rainfall_data.max())

    X, Y, Z = create_density_plot(attack_temps, attack_rainfall, temp_range, rain_range)

    pcm = ax.pcolormesh(X, Y, Z, cmap="RdYlGn_r", shading="auto")
    ax.plot(
        attack_temps,
        attack_rainfall,
        linewidth=0.5,
        marker="x",
        color="black",
        alpha=0.5,
        linestyle="none",
        label="Shark Attacks",
    )

    title = format_title(country, species)
    setup_plot_aesthetics(ax, temp_range, rain_range, title)
    plt.colorbar(pcm, label="Attack Probability Density")
    plt.legend()
    plt.tight_layout()
    return fig


def format_title(country: str, species: str) -> str:
    country_text = "All Countries" if country == "all" else country
    species_text = "All Species" if species == "all" else species
    return f"Shark Attack Risk Distribution - {country_text} ({species_text})"


def filter_dataframe(df: pd.DataFrame, country: str, species: str) -> pd.DataFrame:
    if country != "all":
        df = df[df["country"].str.lower() == country.lower()]
    if species != "all":
        df = df[
            df["shark_species"].fillna("").str.lower().str.contains(species.lower())
        ]
    if df.empty:
        raise ValueError("No data matches the specified country and species filters")
    return df


def get_data(
    country: str, species: str
) -> Tuple[pd.DatetimeIndex, pd.Series, pd.Series]:
    with open("out/shark_incidents.json") as f:
        df = pd.json_normalize(json.load(f))

    df = filter_dataframe(df, country, species)
    dates = pd.DatetimeIndex(pd.to_datetime(df["utc_datetime"]).dt.floor("D"))

    return (
        dates,
        pd.Series(df["weather.temperature_2m"].values, index=dates),
        pd.Series(df["weather.rain"].values, index=dates),
    )


def generate_output_filename(directory: str, country: str, species: str) -> Path:
    base_name = (
        "shark_attacks_global"
        if country == "all" and species == "all"
        else f"shark_attacks_{country}_{species}"
    )
    return Path(directory) / f"{base_name}.png"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot shark attack risk distribution.")
    parser.add_argument(
        "--country", type=str, default="all", help="Filter data by country"
    )
    parser.add_argument(
        "--species", type=str, default="all", help="Filter data by shark species"
    )
    parser.add_argument("--outDir", type=str, default=".", help="Output directory")
    args = parser.parse_args()

    attack_dates, sst_data, rainfall_data = get_data(args.country, args.species)
    fig = plot_shark_attacks(
        sst_data, rainfall_data, attack_dates, args.country, args.species
    )

    output_path = generate_output_filename(args.outDir, args.country, args.species)
    fig.savefig(output_path)
    plt.close(fig)

    print(f"Plot saved to {output_path}")
