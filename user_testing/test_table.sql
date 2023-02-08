drop table public.jc_test;
CREATE TABLE public.jc_test (
fid INT PRIMARY KEY NOT NULL,
geom GEOMETRY(POINT, 4326)
);

INSERT INTO public.jc_test 
VALUES
(1, ST_POINT(-122.48, 37.758, 4326)),
(2, ST_POINT(-123.0, 38.77, 4326)),
(3, ST_POINT(-121.0, 36.77, 4326));

-- curl -X DELETE "http://localhost:9000/collections/public.jc_test/items/1" -H "accept: */*"

Select count(*) from public.jc_test -- needs to be 2 
