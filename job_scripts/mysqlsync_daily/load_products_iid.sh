set -o errexit
set -x

vsql -h vertica.analytics.renttherunway.it -d rtr -U saurabh -w RentTheRunway1 -c "truncate table analytics.products_iid"

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "copy analytics.products_iid from LOCAL 'rtrbi_products_iid.gz' GZIP delimiter E'\t' NULL AS 'NULL' SKIP 1 REJECTMAX 1"

rm rtrbi_products_iid.gz

