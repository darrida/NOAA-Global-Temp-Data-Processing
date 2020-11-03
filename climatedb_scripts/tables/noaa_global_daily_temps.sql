-- Table: climate.noaa_global_daily_temps

-- DROP TABLE climate.noaa_global_daily_temps;

CREATE TABLE climate.noaa_global_daily_temps
(
    uid bigint NOT NULL DEFAULT nextval('climate.noaa_global_daily_temps_uid_seq'::regclass),
    date date NOT NULL,
    station integer NOT NULL,
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
    CONSTRAINT noaa_global_daily_temps_pkey PRIMARY KEY (date, station),
    CONSTRAINT db_id_fkey FOREIGN KEY (station)
        REFERENCES climate.noaa_global_temp_sites (station) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE climate.noaa_global_daily_temps
    OWNER to postgres;