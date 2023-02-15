curl --location --request POST 'http://localhost:9000/collections/public.jc_test/items' \
--header 'Content-Type: application/json' \
--data-raw '{ "type": "Feature", "properties": { "fid": 1}, "geometry": { "type": "Point", "coordinates": [ -75.849253579389796, 47.6434349837781 ] }}'
