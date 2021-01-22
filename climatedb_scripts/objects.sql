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