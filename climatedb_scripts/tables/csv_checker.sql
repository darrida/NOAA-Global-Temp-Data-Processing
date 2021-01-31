-- Table: climate.csv_checker

-- DROP TABLE climate.csv_checker;

CREATE TABLE climate.csv_checker
(
    station character varying COLLATE pg_catalog."default" NOT NULL,
    date_create date NOT NULL,
    date_update date NOT NULL,
    year character varying COLLATE pg_catalog."default" NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE climate.csv_checker
    OWNER to ben;