curl --location --request POST 'http://localhost:9000/collections/public.jc_test/items' \
--header 'Content-Type: application/json' \
--data-raw '{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [ -75.849253579389796, 47.6434349837781 ] } , 
"properties": { "fid": 1}}'
