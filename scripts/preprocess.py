import pandas as pd

data = pd.read_csv("data/accidents.csv")

data.rename(
    columns={
        "Distance(mi)": "Distance",
        "Temperature(F)": "Temperature",
        "Humidity(%)": "Humidity",
        "Pressure(in)": "Pressure",
        "Visibility(mi)": "Visibility",
        "Wind_Speed(mph)": "Wind_speed",
        "Precipitation(in)": "Precipitation",
        "Wind_Chill(F)": "Wind_Chill",
    },
    inplace=True,
)

data.to_csv("data/accidents.csv", index=False)
