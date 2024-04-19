use airportdata;

DROP TABLE IF EXISTS airports;
CREATE TABLE airports {
    Code varchar(3) not null primary key,
    Name varchar(255)
};use airportdata;

DROP TABLE IF EXISTS airports;
CREATE TABLE airports (
    code VARCHAR(3) not null primary key,
    name VARCHAR(255)
);

CREATE TABLE time (
    label VARCHAR(10),
    month INTEGER,
    month_name VARCHAR(20),
    year INTEGER,
    PRIMARY KEY (label, month, year)
);

CREATE TABLE statistics (
    airport_code VARCHAR(3) REFERENCES Airport(code),
    time_label VARCHAR(10) REFERENCES Time(label),
    delays_carrier INTEGER,
    delays_late_aircraft INTEGER,
    delays_national_aviation_system INTEGER,
    delays_security INTEGER,
    delays_weather INTEGER,
    carriers_names TEXT,
    carriers_total INTEGER,
    flights_cancelled INTEGER,
    flights_delayed INTEGER,
    flights_diverted INTEGER,
    flights_on_time INTEGER,
    flights_total INTEGER,
    minutes_delayed_carrier INTEGER,
    minutes_delayed_late_aircraft INTEGER,
    minutes_delayed_national_aviation_system INTEGER,
    minutes_delayed_security INTEGER,
    minutes_delayed_total INTEGER,
    minutes_delayed_weather INTEGER,
    PRIMARY KEY (airport_code, time_label)
);