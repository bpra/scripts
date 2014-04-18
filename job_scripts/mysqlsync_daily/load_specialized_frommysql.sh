echo "Run on `date`"
res1=$(date +%s)
set -u
set -o errexit
set -x

export PATH=$PATH:/opt/vertica/bin/

cd /home/deploy/analytics_scripts/job_scripts/mysqlsync_daily/

rm -f rtrbi_*
# TODO: Make sure you are in the right directory
# TODO: Then rm rtrbi*

# Extract everything from mysql in parallel
#mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select order_date, ship_out_date, rent_begin_date, uid, order_id, order_status, group_id, rid, backup_rid, nid, iid, style_name, size, sku, qty, price_paid from rtrbi.order_simplereservation_reservation_items_snapshot" > rtrbi_orders &

mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select order_date, ship_out_date, rent_begin_date, uid, order_id, order_status, group_id, rid, backup_rid, nid, iid, style_name, size, sku, qty, price_paid from rtrbi.order_simplereservation_reservation_items_snapshot where abs(datediff(order_date, curdate())) < 7" > rtrbi_orders &

mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select order_id, uid, created, modified from rtr_prod0808.uc_orders" >rtrbi_orders_dt &

mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select order_id, uid, promo_id, promo_code from rtrbi.order_attributes" >rtrbi_orders_attr &

mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select prd.combo_type, nullif(prd.special_type, '') special_type, prd.type, prd.node_type, prd.sku_list, prd.mongo_collection, prd.mongo_id, prd.nid, prd.rentable_nid, prd.model, prd.size_list, standard_size_list, prd.designer_id, prd.designer, designer_classification, title, avl_date, avl_fiscal_month, on_site, list_price, prd.cost, sell_price, sub_type, secondary_type, season_code, embellishment, prd.length, prd.neckline, prd.sleeve, prd.style, prd.occasion, prd.color, prd.fabric_code, prd.stretch, prd.body_types, prd.bra_type, prd.component_list, marketing_occasions, fashion_value, total_units, rtv_deactivations, short_ship_deactivations, other_deactivations, auto_late_deactivations, auto_evaluate_deactivations, active_units, sold_units, qty, avl_qty, drupal_active_units, rescal_active_units, rescal_active_barcodes, prd.vendorSKU, prd.is_received, received_date, is_replenishment_style, replenishment_start_date, replenishment_end_date, should_ship, should_return, is_bridesmaid_style, product_url, image_url, product_description, image_count, prd.fitNotes, sizeScaleName, formality, prd.plus_style,prd.moda_style  from rtrbi.products_master prd left outer join rtrbi.mongo_products_master mng on (prd.model = mng.styleName)" >rtrbi_product_master &

#mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select users.*, users_extra.zipcode from rtrbi.users left outer join rtr_prod0808.users_extra using (uid)" >rtrbi_users &
mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select users.* from rtrbi.users" >rtrbi_users &

mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select model as style, sku, size from rtrbi.products_master_iid" > rtrbi_products_iid &

# mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select style_name, score_fw from rtrbi.grid_scoring_prod_scores_1116 where score_trt = 1 and segment_id = 3 and on_site order by score_fw desc" > rtrbi_default_vec &

# PRO tables
mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select * from rtr_prod0808.membership" >rtrbi_membership &
mysql -A -hdb5.analytics.renttherunway.it -usaurabh -p67sxzxje -Danalytics -e "select * from rtr_prod0808.membership_order_history " >rtrbi_membership_order_history &

wait # for it

gzip rtrbi_*



./load_users.sh  # Has to finish first, dependence on merge later
python getUserService.py | gzip > userService.gz
./load_userService.sh 

./load_hearts.sh &
./load_rezcal.sh &
./load_orders.sh &
./load_products.sh &
./load_products_iid.sh &
./load_pro.sh &
#./load_default_vec.sh &
wait