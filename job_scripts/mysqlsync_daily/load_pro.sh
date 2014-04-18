set -o errexit
set -x

vsql -h vertica.analytics.renttherunway.it -d rtr -U saurabh -w RentTheRunway1 -c "set timezone to 'US/Central'; truncate table analytics.membership_terms; copy analytics.membership_terms from LOCAL 'rtrbi_membership.gz' GZIP delimiter E'\t' SKIP 1 REJECTMAX 1 NULL as 'NULL'"

vsql -h vertica.analytics.renttherunway.it -d rtr -U saurabh -w RentTheRunway1 -c "set timezone to 'US/Central'; truncate table analytics.order2membership_terms; copy analytics.order2membership_terms from LOCAL 'rtrbi_membership_order_history.gz' GZIP delimiter E'\t' SKIP 1 REJECTMAX 1 NULL as 'NULL'"


rm rtrbi_membership.gz
rm rtrbi_membership_order_history.gz
