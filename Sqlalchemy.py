# Importowanie niezbędnych modułów
from sqlalchemy import create_engine, update, delete, Column, Float, Integer, String, MetaData, Table, text
import logging
import pandas as pd

logging.getLogger('sqlalchemy.engine').setLevel(logging.CRITICAL)
engine = create_engine('sqlite:///:memory:', echo=False)
metadata = MetaData()
stations_table = Table('stations', metadata,
                       Column('station', String, primary_key=True),
                       Column('latitude', Float), Column('longitude', Float),
                       Column('elevation', Float), Column('name', String),
                       Column('country', String), Column('state', String))


measurements_table = Table('measurements', metadata,
                           Column('id', Integer, primary_key=True),
                           Column('station', String), Column('date', String),
                           Column('prcp', Float), Column('tobs', Float))


metadata.create_all(engine)
print("Created 'stations' and 'measurements' tables.")
path_to_file = r'C:\Users\leszek.stanislawski\Downloads\Kodilla\clean_stations.csv'
stations_df = pd.read_csv(path_to_file)
measurements_df = pd.read_csv(r'C:\Users\leszek.stanislawski\Downloads\Kodilla\clean_measure.csv')


with engine.connect() as connection:
    for index, row in stations_df.iterrows():
        connection.execute(stations_table.insert().values(row.to_dict()))
    print("Data inserted into 'stations' table.")


with engine.connect() as connection:
    for index, row in measurements_df.iterrows():
        mapped_row = {
            'station': row['station'],
            'date': row['date'],
            'prcp': row['precip'], 
            'tobs': row['tobs']
        }
        connection.execute(measurements_table.insert().values(mapped_row))
    print("Data inserted into 'measurements' table.")


def display_stations_table():
    with engine.connect() as connection:
        query = text("SELECT * FROM stations LIMIT 5")
        results_stations = connection.execute(query).fetchall()
        print("Data in 'stations' table:")
        for r in results_stations:
            print(r)


def display_measurements_table():
    with engine.connect() as connection:
        query = text("SELECT * FROM measurements LIMIT 10")
        results_measurements = connection.execute(query).fetchall()
        print("Data in 'measurements' table:")
        for r in results_measurements:
            print(r)
    

if __name__ == "__main__":
    new_measurement = {
        'station': 'USC00512345',
        'date': '2024-01-25',
        'prcp': 0.5,
        'tobs': 70.0
    }
    with engine.connect() as connection:
        connection.execute(measurements_table.insert().values(new_measurement))

    print("Inserted a new record into 'measurements' table.")

    updated_measurement = {
        'station': 'USC00512345',
        'date': '2024-01-25',
        'prcp': 0.8,
        'tobs': 72.0
    }
    with engine.connect() as connection:
        update_stmt = (
            update(measurements_table)
            .where(measurements_table.c.station == 'USC00512345')
            .where(measurements_table.c.date == '2024-01-25')
            .values(updated_measurement)
        )
        connection.execute(update_stmt)

    print("Updated the record in 'measurements' table.")

    with engine.connect() as connection:
        query = text("SELECT * FROM measurements WHERE station = 'USC00512345'")
        results = connection.execute(query).fetchall()
        print("Selected data from 'measurements' table:")
        for r in results:
            print(r)

    with engine.connect() as connection:
        delete_stmt = (
            delete(measurements_table)
            .where(measurements_table.c.station == 'USC00512345')
            .where(measurements_table.c.date == '2024-01-25')
        )
        connection.execute(delete_stmt)

    print("Deleted the record from 'measurements' table.")
