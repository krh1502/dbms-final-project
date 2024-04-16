use airportdata;

DROP TABLE IF EXISTS airports;
CREATE TABLE airports {
    Code varchar(3) not null primary key,
    Name varchar(255)
};