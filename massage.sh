#!/usr/bin/env bash

##########################################
# SCRIPT: massage.sh
##########################################

##########################################
# FILES AND VARIABLES DEFINITION(S)
##########################################
data_dir="$(pwd)/data"
# Set output=false to silence the program
output=true

##########################################
# FUNCTION(S)
##########################################
function verify_prerequisites () {
  if [[ ! -d "${data_dir}" ]]; then
    # Should not reach here; directory should exist
    (>&2 echo "Error: ${data_dir} does not exist")
    exit 1
  fi

  declare -a csv_files=("customer.csv" "district.csv" "item.csv" "order-line.csv" "order.csv" "stock.csv" "warehouse.csv")
  for file in "${csv_files[@]}"; do
    if [[ ! -s "${data_dir}" ]]; then
      # Should not reach here; csv should exist
      (>&2 echo "Error: ${data_dir}/"${file}" does not exist, or has a file size of 0")
      exit 2
    fi
  done
}

function create_new_csv () {
  # Joins Stock with Item
  # Produce StockByWarehouse Column Family
  join -a 1 -j1 2 -j2 1 -t ',' \
       -o 1.1 2.1 2.2 2.3 2.4 2.5 \
          1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 -e "null"\
       "${data_dir}/stock.csv" "${data_dir}/item.csv" > "${data_dir}/cassandra_stockitem.csv"

  if [[ ( "${output}" = true ) && ( "$?" -eq 0 ) ]]; then echo "Created: ${data_dir}/cassandra_stockitem.csv"; fi

  # Joins Warehouse with District
  # Produce (Temporary) Warehouse District
  join -a 1 -j 1 -t ',' \
       -o 1.1 2.2 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 \
          2.3 2.4 2.5 2.6 2.7 2.8 2.9 2.10 2.11 -e "null"\
       "${data_dir}/warehouse.csv" "${data_dir}/district.csv" > "${data_dir}/temp_warehouse_district.csv"

  # Joins Warehouse-District With Customer
  # Produce Customer Column Family
  join -a 1 -j 1 -t ',' \
       -o 1.2 1.3 1.4 2.4 2.5 2.6 2.10 2.12 2.13 2.14 2.18 \
          1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 1.19 1.20 1.21 1.22 -e "null" \
       <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2 "${data_dir}/customer.csv") "${data_dir}/customer.csv" | sort -t',' -k1,1) \
       <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2 "${data_dir}/temp_warehouse_district.csv") "${data_dir}/temp_warehouse_district.csv" | sort -t',' -k1,1) > "${data_dir}/cassandra_customer.csv"

  if [[ ( "${output}" = true ) && ( "$?" -eq 0 ) ]]; then echo "Created: ${data_dir}/cassandra_customer.csv"; fi

  # Change order table, join Customer
  join -a 1 -j 1 -t ','\
    -o 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9\
     2.5 2.6 2.7 -e "null" \
     <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2,4 "${data_dir}/order.csv") "${data_dir}/order.csv" | sort -t',' -k1,1) \
     <(paste -d',' <(cut -d',' --output-delimiter=- -f1,2,3 "${data_dir}/customer.csv") "${data_dir}/customer.csv" | sort -t',' -k1,1) \
    > "${data_dir}/cassandra_order.csv"

  if [[ ( "${output}" = true ) && ( "$?" -eq 0 ) ]]; then echo "Created: ${data_dir}/cassandra_order.csv"; fi

  # Change order-line, join Item
  join -a 1 -j1 5 -j2 1 -t ','\
    -o 1.1 1.2 1.3 1.4 1.5 2.2 1.6 1.7 1.8 1.9 1.10 -e "null"\
    <(cat "${data_dir}/order-line.csv" | sort -t',' -k5,5) <(cat "${data_dir}/item.csv" |sort -t',' -k1,1)\
    > "${data_dir}/cassandra_order-line.csv"

  if [[ ( "${output}" = true ) && ( "$?" -eq 0 ) ]]; then echo "Created: ${data_dir}/cassandra_order-line.csv"; fi


  # Set all other tables with cassandra_
  declare -a unchanged_file=("district.csv" "warehouse.csv")
  for file in "${unchanged_file[@]}"; do
    cp "${data_dir}/${file}" "${data_dir}cassandra_${file}"
    if [[ ( "${output}" = true ) && ( "$?" -eq 0 ) ]]; then echo "Created: ${data_dir}/cassandra_${file}"; fi
  done

  # Cleanup
  rm "${data_dir}/temp_warehouse_district.csv"
}

function append_null () {
  ## Append null
  sed -i 's/$/,null,null,null/' "${data_dir}/cassandra_customer.csv"
  sed -i 's/$/,null/' "${data_dir}/cassandra_district.csv"
  sed -i 's/$/,null,null,null,null/' "${data_dir}/cassandra_order.csv"
}

function extract_id () {
  awk -F "," '{printf("%d\n", $1) }' "${data_dir}/warehouse.csv" >>"${data_dir}/wid_list.csv"
  if [[ ( "${output}" = true ) && ( "$?" -eq 0 ) ]]; then echo "Created: ${data_dir}/wid_list.csv"; fi

  awk -F "," '{print $1 "," $2 "," $3}' "${data_dir}/customer.csv" >>"${data_dir}/cid_list.csv"
  if [[ ( "${output}" = true ) && ( "$?" -eq 0 ) ]]; then echo "Created: ${data_dir}/cid_list.csv"; fi
}

##########################################
# MAIN BODY
##########################################
function main () {
  # Ensure that the data directory exist, and that it contains the necessary files
  verify_prerequisites
  # Create new CSV using the JOIN command
  create_new_csv
  # Append null as placeholder for the newly created csv files
  append_null
  # Extract w_id and customer_id into csv file
  extract_id
}

main
