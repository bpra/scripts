set -o errexit
set -x

# Clear staging
vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "truncate table etl.orders; truncate table etl.orders_dt;truncate table etl.orders_attr;"

# Load all into staging
vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "copy etl.orders from LOCAL 'rtrbi_orders.gz' GZIP delimiter E'\t' NULL AS 'NULL' SKIP 1 REJECTMAX 1"

rm rtrbi_orders.gz

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "copy etl.orders_dt from LOCAL 'rtrbi_orders_dt.gz' GZIP delimiter E'\t' NULL AS 'NULL' SKIP 1 REJECTMAX 1"

rm rtrbi_orders_dt.gz

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "copy etl.orders_attr from LOCAL 'rtrbi_orders_attr.gz' GZIP delimiter E'\t' NULL AS 'NULL' SKIP 1 REJECTMAX 1"

rm rtrbi_orders_attr.gz

# Load into final tables

vsql -h vertica.analytics.renttherunway.it -d rtr -U saurabh -w RentTheRunway1 -f /home/deploy/analytics_scripts/job_scripts/mysqlsync_daily/insert_orders.sql

# truncate and load market_basket based on orders
# vsql -h vertica.analytics.renttherunway.it -d rtr -U saurabh -w RentTheRunway1 -f insert_market_basket.sql

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -f /home/deploy/analytics_scripts/job_scripts/mysqlsync_daily/merge_orders2users.sql

