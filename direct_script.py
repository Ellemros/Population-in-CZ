import sqlite3
import json
import os
import urllib.parse
import urllib.request


DB_FILE = 'population_data.db'  
API_KEY = os.getenv('API_KEY_DIRECT')

def create_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("create table if not exists DimRegion (RegionID integer primary key, Name text)")
    
    cursor.execute("create table if not exists DimDistrict (DistrictID integer primary key, Name text)")

    cursor.execute("create table if not exists DimOrp (OrpID integer primary key, Name text)")

    cursor.execute("create table if not exists DimLocation (LocationID integer primary key, Name text)")

    cursor.execute("create table if not exists DimStatistika (StatistikaID integer primary key, Description text)")

    cursor.execute("create table if not exists DimTime (TimeID integer primary key AUTOINCREMENT, Year integer, Date text, UNIQUE(Year, Date))")

    cursor.execute("""create table if not exists FactPopulation (
            Idhod integer primary key,
            RegionID integer,
            DistrictID integer,
            OrpID integer,
            LocationID integer,
            StatistikaID integer,
            TimeID integer,
            Population integer,
            foreign key (RegionID) references DimRegion(RegionID),
            foreign key (DistrictID) references DimDistrict(DistrictID),
            foreign key (OrpID) references DimOrp(OrpID),
            foreign key (LocationID) references DimLocation(LocationID),
            foreign key (StatistikaID) references DimStatistika(StatistikaID),
            foreign key (TimeID) references DimTime(TimeID))""")

    conn.commit()
    conn.close()

def download_population_data():
    data = []
    skip = 0
    limit = 30

    while True:
        filter = {
            "where": {
                "kraj": {"inq": [3115, 3018]}
            },
            "limit": limit,
            "skip": skip
        }

        query = urllib.parse.urlencode({'filter': json.dumps(filter)})
        url = f"https://api.apitalks.store/czso.cz/obyvatelstvo-domy?{query}"

        req = urllib.request.Request(url)
        req.add_header('x-api-key', API_KEY)

        with urllib.request.urlopen(req) as response:
            raw_data = response.read()
            result = json.loads(raw_data.decode("utf-8"))
            page = result["data"]
            if not page:
                break
            data.extend(page)

        skip += limit

    return data


def insert_into_database(data):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for row in data:
        cursor.execute("insert or ignore into DimRegion (RegionID, Name) values (?, ?)", (row["kraj"], row["kraj_text"]))
        cursor.execute("insert or ignore into DimDistrict (DistrictID, Name) values (?, ?)", (row["okres"], row["okres_text"]))
        cursor.execute("insert or ignore into DimOrp (OrpID, Name) values (?, ?)", (row["so_orp"], row["so_orp_text"]))
        cursor.execute("insert or ignore into DimLocation (LocationID, Name) values (?, ?)", (row["vuzemi_kod"], row["vuzemi_txt"]))
        cursor.execute("insert or ignore into DimStatistika (StatistikaID, Description) values (?, ?)", (row["stapro_kod"], row["stapro_txt"]))
        cursor.execute("insert or ignore into DimTime (Year, Date) values (?, ?)", (row["rok"], row["datum"]))

        cursor.execute("select TimeID from DimTime where Year = ? and Date = ?", (row["rok"], row["datum"]))
        time_id = cursor.fetchone()[0]

        cursor.execute("""insert or ignore into FactPopulation (Idhod, RegionID, DistrictID, OrpID, LocationID, StatistikaID, TimeID, Population)
                        values (?, ?, ?, ?, ?, ?, ?, ?)""", 
                        (row["idhod"], row["kraj"], row["okres"], row["so_orp"], row["vuzemi_kod"], row["stapro_kod"], time_id, row["hodnota"]))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))  
    create_database()
    data = download_population_data()
    insert_into_database(data)