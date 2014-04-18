set -o errexit
set -x

# vsql -h vertica01.analytics.renttherunway.it -d rtr -U saurabh -w RentTheRunway1 -c "truncate table etl.userservice"

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "truncate table etl.userservice; copy etl.userservice from LOCAL 'userService.gz' GZIP delimiter ',' NULL AS ' ' REJECTMAX 10"

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -f merge_userService.sql

rm userService.gz

