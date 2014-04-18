set -o errexit
set -x

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "truncate table etl.users; copy etl.users(uid, mail, created, create_date FORMAT 'yyyy-mm-dd' NULL 'NULL', is_rtr_email, first_name, last_name, x3 FILLER varchar, dob_date FORMAT 'yyyy-mm-dd' NULL 'NULL', x2 FILLER varchar, profile_referral NULL 'NULL', zip_users) from LOCAL 'rtrbi_users.gz' GZIP delimiter E'\t' SKIP 1 REJECTMAX 1"

vsql -h vertica.analytics.renttherunway.it -d rtr -U etl -w Living4Data -c "insert into analytics.users select stage.* from etl.users stage left outer join analytics.users tgt using (uid) where tgt.uid is null; commit; "

rm rtrbi_users.gz
