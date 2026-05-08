INSERT INTO city_county_lookup (city, state, county_name)
VALUES
    ('Wooster', 'OH', 'Wayne'),
    ('Orrville', 'OH', 'Wayne'),
    ('Rittman', 'OH', 'Wayne'),
    ('Ashland', 'OH', 'Ashland'),
    ('Loudonville', 'OH', 'Ashland'),
    ('Millersburg', 'OH', 'Holmes'),
    ('Berlin', 'OH', 'Holmes'),
    ('Medina', 'OH', 'Medina'),
    ('Wadsworth', 'OH', 'Medina'),
    ('Canton', 'OH', 'Stark'),
    ('Massillon', 'OH', 'Stark'),
    ('Alliance', 'OH', 'Stark')
ON CONFLICT (city, state)
DO UPDATE SET
    county_name = EXCLUDED.county_name;