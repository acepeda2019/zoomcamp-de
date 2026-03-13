# Input arguments:
# Usage: ./download_data.sh yellow 2020
TAXI_TYPE=($1 "yellow") #"yellow default"
YEAR=($2 2020) #2020 default

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

# Example URLs
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2020-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2020-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2020-01.parquet


for month in $(seq 1 12);
do
    MONTH=$(printf "%02d" $month)
    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.parquet"

    LOCAL_PREFIX="data/parquet/${TAXI_TYPE}/${YEAR}/${MONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo downloading ${URL} to ${LOCAL_PATH}
    mkdir -p ${LOCAL_PREFIX}
    wget ${URL} -O ${LOCAL_PATH}
done
