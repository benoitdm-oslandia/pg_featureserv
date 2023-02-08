curl --location --request PATCH 'http://localhost:9000/collections/public.jc_test/items/2' \
--header 'Content-Type: application/json' \
--data-raw '{ "type": "Feature",
  "geometry": {
    "coordinates": [
      -70.88461956597838,
      47.807897059236495
    ],
    "type": "Point"
  },
  "properties": {
    "fid" : 1
  }
}'