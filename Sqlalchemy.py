# Ensure that the correct version of SQLAlchemy is used
import sqlalchemy
import logging
from sqlalchemy import create_engine, Column, Float, Integer, String, MetaData, Table, text
import pandas as pd
import os

# Ustawienia konfiguracji loggera
logging.basicConfig(filename='alchemycheck.txt', level=logging.INFO)

path_to_file = r'C:\Users\leszek.stanislawski\Downloads\Kodilla\clean_stations.csv'

if os.path.exists(path_to_file):
    print("Plik istnieje.")
    logging.info("Plik istnieje.")
else:
    print("Plik nie istnieje.")
    logging.error("Plik nie istnieje.")

# Tworzymy instancję silnika dla bazy danych SQLite w pamięci
engine = create_engine('sqlite:///:memory:', echo=True)

# Definiujemy tabelę "stations"
metadata = MetaData()
stations_table = Table('stations', metadata,
                       Column('station', String, primary_key=True),
                       Column('latitude', Float), Column('longitude', Float),
                       Column('elevation', Float), Column('name', String),
                       Column('country', String), Column('state', String))

# Definiujemy tabelę "measurements"
measurements_table = Table('measurements', metadata,
                           Column('id', Integer, primary_key=True),
                           Column('station', String), Column('date', String),
                           Column('prcp', Float), Column('tobs', Float))

# Tworzymy tabelę "stations" i "measurements"
metadata.create_all(engine)
print("Created 'stations' and 'measurements' tables.")
logging.info("Created 'stations' and 'measurements' tables.")

# Wczytujemy dane z plików CSV
stations_df = pd.read_csv(path_to_file)
measurements_df = pd.read_csv(
    r'C:\Users\leszek.stanislawski\Downloads\Kodilla\clean_measure.csv')

# Wstawiamy dane do tabeli "stations"
with engine.connect() as connection:
    stations_df.to_sql('stations', connection, if_exists='replace', index=False)
    print("Data inserted into 'stations' table.")
    logging.info("Data inserted into 'stations' table.")

# Wstawiamy dane do tabeli "measurements"
with engine.connect() as connection:
    measurements_df.to_sql('measurements',
                           connection,
                           if_exists='replace',
                           index=False)
    print("Data inserted into 'measurements' table.")
    logging.info("Data inserted into 'measurements' table.")

# Metoda do wyświetlania tabeli "stations"
def display_stations_table():
    with engine.connect() as connection:
        query = text("SELECT * FROM stations LIMIT 5")
        results_stations = connection.execute(query).fetchall()
        print("Data in 'stations' table:")
        for r in results_stations:
            print(r)

# Metoda do wyświetlania tabeli "measurements"
def display_measurements_table():
    with engine.connect() as connection:
        query = text("SELECT * FROM measurements LIMIT 10")
        results_measurements = connection.execute(query).fetchall()
        print("Data in 'measurements' table:")
        for r in results_measurements:
            print(r)

# Przykładowe wywołanie nowych metod
display_stations_table()
display_measurements_table()
