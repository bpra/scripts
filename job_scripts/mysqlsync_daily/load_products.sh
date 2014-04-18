set -o errexit
set -x

vsql -h vertica.analytics.renttherunway.it -d rtr -U saurabh -w RentTheRunway1 -c "truncate table analytics.products"

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "copy analytics.products from LOCAL 'rtrbi_product_master.gz' GZIP delimiter E'\t' NULL AS 'NULL' SKIP 1 REJECTMAX 1"

rm rtrbi_product_master.gz

