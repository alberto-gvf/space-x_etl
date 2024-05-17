INSERT INTO public.cores (
    core_id,
    flight,
    gridfins,
    legs,
    reused,
    landing_attempt,
    landing_success,
    landing_type,
    landpad,
    launch_id,
    p_creation_date
)
    SELECT
        core_id,
        flight,
        gridfins,
        legs,
        reused,
        landing_attempt,
        landing_success,
        landing_type,
        landpad,
        launch_id,
        p_creation_date::date
        FROM {temp_table_name}
        ON CONFLICT (core_id, launch_id) DO UPDATE
        SET
            flight = EXCLUDED.flight,
            gridfins = EXCLUDED.gridfins,
            legs = EXCLUDED.legs,
            reused = EXCLUDED.reused,
            landing_attempt = EXCLUDED.landing_attempt,
            landing_success = EXCLUDED.landing_success,
            landing_type = EXCLUDED.landing_type,
            landpad = EXCLUDED.landpad,
            p_creation_date = EXCLUDED.p_creation_date;