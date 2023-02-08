drop table public.vt_test;
create table public.vt_test (
geom geometry(polygon, 4326),
fid serial primary key,
release int,
capture_dates_range varchar
);