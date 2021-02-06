CREATE TABLE IF NOT EXISTS climate.csv_checker (
  station CHARACTER VARYING NOT NULL,
  date_create DATE NOT NULL,
  date_update DATE NOT NULL,
  year CHARACTER VARYING NOT NULL,
  CONSTRAINT csv_checker_pkey PRIMARY KEY (station, year)
) USING HEAP;

CREATE TABLE IF NOT EXISTS climate.noaa_global_temp_sites (
  station BIGINT NOT NULL,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  elevation DOUBLE PRECISION,
  name CHARACTER VARYING,
  CONSTRAINT noaa_global_temp_sites_pkey PRIMARY KEY (station)
) USING HEAP;

CREATE SEQUENCE IF NOT EXISTS climate.noaa_global_daily_temps_uid_seq;

CREATE TABLE IF NOT EXISTS climate.noaa_global_daily_temps (
  uid BIGINT NOT NULL DEFAULT nextval('climate.noaa_global_daily_temps_uid_seq' :: REGCLASS),
  date DATE NOT NULL,
  station BIGINT NOT NULL,
  temp DOUBLE PRECISION NOT NULL,
  temp_attributes INTEGER,
  dewp DOUBLE PRECISION,
  dewp_attributes INTEGER,
  slp DOUBLE PRECISION,
  slp_attributes INTEGER,
  stp DOUBLE PRECISION,
  stp_attributes INTEGER,
  visib DOUBLE PRECISION,
  visib_attributes INTEGER,
  wdsp DOUBLE PRECISION,
  wdsp_attributes INTEGER,
  mxspd DOUBLE PRECISION,
  gust DOUBLE PRECISION,
  max DOUBLE PRECISION,
  max_attributes CHARACTER VARYING,
  min DOUBLE PRECISION,
  min_attributes CHARACTER VARYING,
  prcp DOUBLE PRECISION,
  prcp_attributes CHARACTER VARYING,
  sndp DOUBLE PRECISION,
  frshtt INTEGER,
  CONSTRAINT noaa_global_daily_temps_pkey PRIMARY KEY (date, station)
) USING HEAP;


  -- Table: climate.noaa_global_daily_temps

-- DROP TABLE climate.noaa_global_daily_temps;

-- Table: climate.noaa_global_daily_temps

-- DROP TABLE climate.noaa_global_daily_temps;

CREATE TABLE climate.noaa_global_daily_temps
(
    uid serial primary key,
    date date NOT NULL,
    station bigint NOT NULL,
  	latitude DOUBLE PRECISION,
  	longitude DOUBLE PRECISION,
  	elevation DOUBLE PRECISION,
    temp double precision NOT NULL,
    temp_attributes integer,
    dewp double precision,
    dewp_attributes integer,
    slp double precision,
    slp_attributes integer,
    stp double precision,
    stp_attributes integer,
    visib double precision,
    visib_attributes integer,
    wdsp double precision,
    wdsp_attributes integer,
    mxspd double precision,
    gust double precision,
    max double precision,
    max_attributes character varying COLLATE pg_catalog."default",
    min double precision,
    min_attributes character varying COLLATE pg_catalog."default",
    prcp double precision,
    prcp_attributes character varying COLLATE pg_catalog."default",
    sndp double precision,
    frshtt integer,
	name CHARACTER VARYING,
    CONSTRAINT unique_station_date UNIQUE (station, date)
        INCLUDE(date, station)
)

TABLESPACE pg_default;

ALTER TABLE climate.noaa_global_daily_temps
    OWNER to ben;