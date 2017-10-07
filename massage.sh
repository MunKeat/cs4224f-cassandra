#!/usr/bin/env bash

############################################
# Joins Stock with Item
# Produce StockByWarehouse Column Family
#
# Assumes the following for schema
#
# Stock.csv:
# 1. S_W_ID
# 2. S_I_ID
# 3. S_QUANTITY
# 4. S_YTD
# 5. S_ORDER_CNT
# 6. S_REMOTE_CNT
# 7. S_DIST_01
# 8. S_DIST_02
# 9. S_DIST_03
# 10. S_DIST_04
# 11. S_DIST_05
# 12. S_DIST_06
# 13. S_DIST_07
# 14. S_DIST_08
# 15. S_DIST_09
# 16. S_DIST_10
# 17. S_DATA
#
# Item.csv:
# 1. I_ID
# 2. I_NAME
# 3. I_PRICE
# 4. I_IM_ID
# 5. I_DATA
############################################
join -a 1 -j1 2 -j2 1 -t ',' \
     -o 1.1 2.1 2.2 2.3 2.4 2.5 \
        1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 -e "null"\
     ./stock.csv ./item.csv > ./cassandra_stockitem.csv

############################################
# Joins Warehouse with District
# Produce (Temporary) Warehouse District
#
# Assumes the following for schema
#
# Warehouse.csv
# 1. W_ID
# 2. W_NAME
# 3. W_STREET_2
# 4. W_STREET_1
# 5. W_CITY
# 6. W_STATE
# 7. W_ZIP
# 8. W_TAX
# 9. W_YTD
#
# District.csv:
# 1. D_W_ID
# 2. D_ID
# 3. D_NAME
# 4. D_STREET_1
# 5. D_STREET_2
# 6. D_CITY
# 7. D_STATE
# 8. D_ZIP
# 9. D_TAX
# 10. D_YTD
# 11. D_NEXT_O_ID
############################################
join -a 1 -j 1 -t ',' \
     -o 1.1 2.2 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 \
        2.3 2.4 2.5 2.6 2.7 2.8 2.9 2.10 2.11 -e "null"\
     ./warehouse.csv ./district.csv > ./temp_warehouse_district.csv

############################################
# Joins Warehouse-District With Customer
# Produce Customer Column Family
#
# Assumes the following for schema
#
# customer.csv:
# 1. C_W_ID
# 2. C_D_ID
# 3. C_ID
# 4. C_FIRST
# 5. C_MIDDLE
# 6. C_LAST
# 7. C_STREET_1
# 8. C_STREET_2
# 9. C_CITY
# 10. C_STATE
# 11. C_ZIP
# 12. C_PHONE
# 13. C_SINCE
# 14. C_CREDIT
# 15. C_CREDIT_LIM
# 16. C_DISCOUNT
# 17. C_BALANCE
# 18. C_YTD_PAYMENT
# 19. C_PAYMENT_CNT
# 20. C_DELIVERY_CNT
# 21. C_DATA
############################################
join -a 1 -j 1 -t ',' \
     -o 1.2 1.3 1.4 2.4 2.5 2.6 2.10 2.12 2.13 2.14 2.18 \
        1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 1.21 1.22 -e "null" \
     <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2 ./customer.csv) ./customer.csv | sort -t',' -k1,1) \
     <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2 ./temp_warehouse_district.csv) ./temp_warehouse_district.csv | sort -t',' -k1,1) > ./cassandra_customer.csv

############################################
# Current work-in-progress
#
# Set all other tables with cassandra_ 
############################################
declare -a unchanged_file=("district.csv" "warehouse.csv")

for file in "${unchanged_file[@]}"; do
	cp "$file" cassandra_"$file"
done


#######
# change order table, join Customer
#####
join -a 1 -j 1 -t ','\
	-o 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9\
	 2.5 2.6 2.7 -e "null" \
	 <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2,4 ./order.csv) ./order.csv | sort -t',' -k1,1) \
	 <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2,3 ./customer.csv) ./customer.csv | sort -t',' -k1,1) \
	> cassandra_order.csv
####
# change order-line, join Item
#####
join -a 1 -j1 5 -j2 1 -t ','\
	-o 1.1 1.2 1.3 1.4 1.5 2.2 1.6 1.7 1.8 1.9 1.10 -e "null"\
	<(cat order-line.csv| sort -t',' -k5,5) <(cat item.csv|sort -t',' -k1,1)\
	> cassandra_order-line.csv
	


## Append null

sed -i 's/$/,null,null,null/' cassandra_customer.csv
sed -i 's/$/,null/' cassandra_district.csv
sed -i 's/$/,null,null,null,null/' cassandra_order.csv


# Cleanup
rm ./temp_warehouse_district.csv

