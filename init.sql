CREATE TABLE public.launches
(
    launch_id character varying(32),
    success boolean,
    flight_number integer,
    name character varying(255),
    date_utc timestamp without time zone,
    date_local timestamp without time zone,
    date_precision character varying(5),
    p_creation_date date,
    PRIMARY KEY (launch_id)
);

CREATE TABLE public.cores
(
    core_id character varying(32),
    flight integer,
    gridfins boolean,
    legs boolean,
    reused boolean,
    landing_attempt boolean,
    landing_success boolean,
    landing_type character varying(32),
    landpad character varying(32),
    launch_id character varying(32),
    p_creation_date date,
    PRIMARY KEY (core_id, launch_id)
);



