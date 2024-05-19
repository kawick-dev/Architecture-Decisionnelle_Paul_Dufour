

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