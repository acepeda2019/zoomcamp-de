gcloud storage cat gs://dtc-de-course-488600-batch/code/06_spark_sql.py

# COPY GREEN DATA TO GCS
gcloud storage cp -r \
    /Users/acepeda/Documents/GitHub/zoomcamp-de/06-batch/data/parquet_spark/green \
    gs://dtc-de-course-488600-batch/green

# COPY CODE TO GCS
gcloud storage cp \
    /Users/acepeda/Documents/GitHub/zoomcamp-de/06-batch/code/06_spark_sql_gcs.py \
    gs://dtc-de-course-488600-batch/code/06_spark_sql_gcs.py

gcloud storage cp \
    /Users/acepeda/Documents/GitHub/zoomcamp-de/06-batch/code/06_spark_sql_bigquery.py \
    gs://dtc-de-course-488600-batch/code/06_spark_sql_bigquery.py


# SUBMIT JOB TO DATAPROCgs://dtc-de-course-488600-batch/green/green_tripdata_merged.parquet
gcloud dataproc jobs submit pyspark \
    --cluster=zoomcamp \
    --region=us-central1 \
    gs://dtc-de-course-488600-batch/code/06_spark_sql.py \
    -- \
        --input-green="gs://dtc-de-course-488600-batch/green/*/*" \
        --input-yellow="gs://dtc-de-course-488600-batch/yellow/2020/*/*" \
        --output=gs://dtc-de-course-488600-batch/revenue


gcloud dataproc jobs submit pyspark \
    --cluster=zoomcamp \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc-de-course-488600-batch/code/06_spark_sql_bigquery.py \
    -- \
        --input-green="gs://dtc-de-course-488600-batch/green/*/*" \
        --input-yellow="gs://dtc-de-course-488600-batch/yellow/2020/*/*" \
        --output=trips_data_all.revenue_report



# ------------------------------------------------------------
# DEBUGGING
# ------------------------------------------------------------

# UPDATE CLUSTER TO USE 4 WORKERS - didn't work, no workers were added
gcloud dataproc clusters update zoomcamp \
      --region=us-central1 \
      --num-workers=4

# RECREATE CLUSTER
gcloud dataproc clusters delete zoomcamp --region=us-central1                                     

# HBD Quota hit, didn't work                                                                                 
gcloud dataproc clusters create zoomcamp \
    --region=us-central1 \
    --num-workers=2 \
    --worker-machine-type=n4-standard-2 \
    --master-machine-type=n4-standard-2   

# Single node cluster
gcloud dataproc clusters create zoomcamp \
    --max-idle=20m \
    --max-age=3h \
    --region=us-central1 \
    --single-node \
    --master-machine-type=n2-standard-4 