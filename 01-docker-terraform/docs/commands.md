## Docker Commands
```bash
docker build -t <image-name:tag>          # Build an image with a name:tag
docker run <image-name:tag>               # Spin up a docker container 
  -it                                     # Interactive, allows you to interact with container
  --rm                                    # Remove the container once process is complete, keeps space clean

docker ps                                 # list all ACTIVE docker containers
  -a                                      # list all docker containers
  -q                                      # return the id only

docker rm <container_id>                  # remove one container 
docker rm <id1><id2>                      # remove multiple containers
docker rm prune                           # remove ALL stopped containers at once

docker rm $(docker ps -aq)                # remove ALL docker container (Active and Inactive)

docker network create <pg-network>        # Create docker network
docker network ls                         # List all networks
docker network rm <pg-network>            # Remove one network

docker compose up
docker compose down
```

## Postgres Comnmands
```bash
# Spin up a postgres container on port 5432 
# Mount volumne ny_taxi_postgres_data to /var/../postgresql in the container
docker run -it --rm \
    -e POSTGRES_USER=root \
    -e POSTGRES_PASSWORD=root \
    -e POSTGRES_DB=ny_taxi \
    -v ny_taxi_postgres_data:/var/lib/postgresql/ \
    -p 5432:5432 \
    postgres:18



# Connect to postgres in Container (Use a separate terminal)
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
  ```

## Python Commands
```bash
## Ingest data into PostgreSQL wit  h parameters
python ingest_data.py \
  --pg-user root \
  --pg-pass root \
  --pg-host localhost \
  --pg-port 5432 \
  --pg-db ny_taxi \
  --target-table yellow_taxi_data \
  --chunksize 100000 \
  --year 2021 \
  --month 1
```
## Find everything in PATH
echo $PATH | tr ':' '\n'

## Ingest data into PostgreSQL with parameters
python ingest_data.py \
    --pg-user root \
    --pg-pass root \
    --pg-host localhost \
    --pg-port 5432 \
    --pg-db ny_taxi \
    --target-table yellow_taxi_data \
    --chunksize 100000 \
    --year 2021 \
    --month 1


## PGADMIN CONTAINER
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  dpage/pgadmin4



 # VERIFY THESE WORK IN A SEPARATE TERMINAL
 ### Run PostgreSQL on the network
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18

### In another terminal, run pgAdmin on the same network
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4

**Have AI create a docker-compose.yml file for the project**

**Run the docker compose file**
```docker compose up```


### Run the pipeline inside the docker network
```bash
docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --month=2 \
    --target-table=yellow_taxi_test

 # spin down the docker network 
 docker compose down
 ```