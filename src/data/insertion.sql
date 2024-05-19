

CREATE TABLE Payments(
    Payment_id SERIAL PRIMARY KEY,
    Payment_type text
);

CREATE TABLE Company(
    Company_id SERIAL PRIMARY KEY,
    Company_name text
);

CREATE TABLE Zone(
    Zone_id SERIAL PRIMARY KEY,
    Zone_Borough text,
    Zone_name TEXT
);

CREATE TABLE Airport_tax(
    Airport_tax_id SERIAL PRIMARY KEY,
    Airport_tax_amount int
);

CREATE TABLE Course(
    Course_id SERIAL PRIMARY KEY,
    Company_id int REFERENCES Company(Company_id),
    tpep_pickup_datetime timestamp without time zone,
    tpep_dropoff_datetime timestamp without time zone,
    passenger_count double precision,
    trip_distance double precision,
    Ratecode_id double precision,
    store_and_fwd_flag text,
    PULocation_id int REFERENCES Zone(Zone_id),
    DOLocation_id int REFERENCES Zone(Zone_id),
    Payment_type int REFERENCES Payments(Payment_id),
    fare_amount double precision,
    extra double precision,
    mta_tax double precision,
    tip_amount double precision,
    tolls_amount double precision,
    improvement_surcharge double precision,
    total_amount double precision,
    congestion_surcharge double precision,
    airport_fee int REFERENCES Airport_tax(Airport_tax_id),
    CONSTRAINT fk_company_id FOREIGN KEY (Company_id) REFERENCES Company(Company_id),
    CONSTRAINT fk_PULocation_id FOREIGN KEY (PULocation_id) REFERENCES Zone(Zone_id),
    CONSTRAINT fk_DOLocation_id FOREIGN KEY (DOLocation_id) REFERENCES Zone(Zone_id),
    CONSTRAINT fk_Payment_type FOREIGN KEY (Payment_type) REFERENCES Payments(Payment_id),
    CONSTRAINT fk_airport_fee FOREIGN KEY (airport_fee) REFERENCES Airport_tax(Airport_tax_id)
);

INSERT INTO company (company_name) VALUES ('Creative Mobile Technologies'),  ('VeriFone Inc');
INSERT INTO payments (payment_type) VALUES ('Credit Card'), ('Cash'), ('No charge'), ('Dispute'), ('Unknown'), ('Voided trip');
INSERT INTO airport_tax (Airport_tax_amount) VALUES (1.25);

#Jouer le script d'insertion 'scipt_add_all_zone.py' pour ajouter les zones de la ville de New York



#Requete pour le transfert de la base de données nyc_taxi vers la base de données nyc_taxi_warehouse


INSERT INTO course (
    Company_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    PULocation_id,
    DOLocation_id,
    Payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
)
SELECT 
    r.vendorid,
    r.tpep_pickup_datetime,
    r.tpep_dropoff_datetime,
    r.passenger_count,
    r.trip_distance,
    r.ratecodeid,
    r.store_and_fwd_flag,
    r.pulocationid,
    r.dolocationid,
    r.payment_type,
    r.fare_amount,
    r.extra,
    r.mta_tax,
    r.tip_amount,
    r.tolls_amount,
    r.improvement_surcharge,
    r.total_amount,
    r.congestion_surcharge,
    r.airport_fee
FROM nyc_raw as r;