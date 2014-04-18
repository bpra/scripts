set -x

mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "source extract_rezcal.sql" >rtrbi_rezcal

gzip -f rtrbi_rezcal

vsql -h vertica.analytics.renttherunway.it -d rtr -U saurabh -w RentTheRunway1 -c "truncate table analytics.rezcal; copy analytics.rezcal from LOCAL 'rtrbi_rezcal.gz' GZIP delimiter E'\t' REJECTMAX 10 NULL as 'NULL' SKIP 1"

rm rtrbi_rezcal.gz

