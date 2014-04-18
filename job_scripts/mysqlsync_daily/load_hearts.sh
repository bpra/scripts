set -o errexit
set -x


rm -f rtrprd_hearts*

mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select * from rtr_prod0808.user_product_rating" >rtrprd_hearts

gzip -f rtrprd_hearts

# Hearts table is in EST, saved without timezone information

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "set timezone to 'US/Eastern'; truncate table etl.user_product_rating; copy etl.user_product_rating from LOCAL 'rtrprd_hearts.gz' GZIP delimiter E'\t' REJECTMAX 100"

rm rtrprd_hearts.gz
