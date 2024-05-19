from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Sequence
from sqlalchemy.orm import sessionmaker
import csv

# Paramètres de connexion à la base de données
db_user = 'postgres'
db_password = 'admin'
db_host = 'localhost'
db_port = '15432'
db_name = 'nyc_datamart'

# Chemin vers le fichier CSV et nom de la table
csv_file_path = 'C:\\Users\\Paul\\Desktop\\NYC-DATA\\ATL-Datamart\\src\\data\\taxi_zone_lookup.csv'
table_name = 'zone'

# Création du moteur SQLAlchemy pour la connexion à la base de données PostgreSQL
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Création de la session
Session = sessionmaker(bind=engine)
session = Session()

# Création d'un objet MetaData
metadata = MetaData()

# Définition de la structure de la table
zones_table = Table(
    table_name,
    metadata,
    Column('zone_id', Integer, Sequence('zone_zone_id_seq'), primary_key=True),
    Column('zone_borough', String),
    Column('zone_name', String)
)

# Création de la table s'il elle n'existe pas déjà
metadata.create_all(engine)

# Lecture du fichier CSV et insertion des données dans la base de données
with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        new_zone = zones_table.insert().values(
            zone_borough=row['zone_borough'],
            zone_name=row['zone_name']
        )
        session.execute(new_zone)

# Validation des changements et fermeture de la session
session.commit()
session.close()

print("Les données ont été chargées avec succès dans la table {}.".format(table_name))