INSERT INTO public.launches (
    launch_id,
    success,
    flight_number,
    name,
    date_utc,
    date_local,
    date_precision,
    p_creation_date
)
    SELECT
        launch_id,
        success,
        flight_number,
        name,
        date_utc,
        date_local::timestamp,
        date_precision,
        p_creation_date::date
    FROM {temp_table_name}
    ON CONFLICT (launch_id)
    DO UPDATE SET
        success = EXCLUDED.success,
        flight_number = EXCLUDED.flight_number,
        name = EXCLUDED.name,
        date_utc = EXCLUDED.date_utc,
        date_local = EXCLUDED.date_local,
        date_precision = EXCLUDED.date_precision,
        p_creation_date = EXCLUDED.p_creation_date;