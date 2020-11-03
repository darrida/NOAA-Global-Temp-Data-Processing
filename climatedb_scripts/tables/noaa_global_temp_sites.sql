-- Table: climate.noaa_global_temp_sites

-- DROP TABLE climate.noaa_global_temp_sites;

CREATE TABLE climate.noaa_global_temp_sites
(
    station integer NOT NULL,
    latitude double precision,
    longitude double precision,
    elevation double precision,
    name character varying COLLATE pg_catalog."default",
    CONSTRAINT noaa_global_temp_sites_pkey PRIMARY KEY (station)
)

TABLESPACE pg_default;

ALTER TABLE climate.noaa_global_temp_sites
    OWNER to postgres;