#!/bin/bash

curl -v -G "http://localhost:8081/download" \
  -H "X-M-Api-Key: change-me" \
  -H "X-M-Api-Secret: change-me" \
  --data-urlencode "filename=FacturaElectronica_50622122500310191554500100001010000000001192784326.html" \
  --data-urlencode "tar_path=/gcp-bucket/edocs/stg-modula-00001/25/12/22/001_12-54.tar.gz"
