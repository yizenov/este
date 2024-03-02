<meta name="robots" content="noindex,nofollow">

# PostgreSQL(v15.1) docker container setup details
We provide the instructions on how to setup a docker container with the PostgreSQL engine to collect its true and estimated cardinalities. In addition, the container can be further configured to be used to compare runtimes of different query plans.

## Table of Contents
1. [Cardinality Docker Container](#cardinality)
2. [Runtime Docker Container](#runtime)

The final docker container includes the following:
- loaded IMDB dataset

## 1. Setting up a PostgreSQL docker container to collect true and estimated cardinalities <a name="cardinality"></a>
- `docker pull postgres:latest`
- `mkdir $HOME/postgres_volume`
- `docker run --name mst_pg_docker --shm-size 128gb -e POSTGRES_PASSWORD=docker -v $HOME/postgres_volume:/home postgres:latest`
- `Control + C` to exit
- `docker start mst_pg_docker` to start the container
- transfer the imdb data into the container:
    * `docker cp $HOME/updated_imdb/aka_name.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/aka_title.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/cast_info.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/char_name.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/company_name.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/company_type.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/comp_cast_type.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/complete_cast.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/info_type.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/keyword.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/kind_type.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/link_type.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/movie_companies.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/movie_info_idx.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/movie_keyword.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/movie_link.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/name.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/role_type.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/title.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/movie_info.csv mst_pg_docker:/home;`
    * `docker cp $HOME/updated_imdb/person_info.csv mst_pg_docker:/home;`
- `docker exec -it mst_pg_docker bash`
- `psql -h localhost -U postgres -d postgres`
- `CREATE DATABASE imdb;`
- `\q`
- `psql -h localhost -U postgres -d imdb`
- Import data into tables
    * `copy aka_name from '/home/aka_name.csv' delimiter '|' null '';`
    * `copy aka_title from '/home/aka_title.csv' delimiter '|' null '';`
    * `copy cast_info from '/home/cast_info.csv' delimiter '|' null '';`
    * `copy char_name from '/home/char_name.csv' delimiter '|' null '';`
    * `copy company_name from '/home/company_name.csv' delimiter '|' null '';`
    * `copy company_type from '/home/company_type.csv' delimiter '|' null '';`
    * `copy comp_cast_type from '/home/comp_cast_type.csv' delimiter '|' null '';`
    * `copy complete_cast from '/home/complete_cast.csv' delimiter '|' null '';`
    * `copy info_type from '/home/info_type.csv' delimiter '|' null '';`
    * `copy keyword from '/home/keyword.csv' delimiter '|' null '';`
    * `copy kind_type from '/home/kind_type.csv' delimiter '|' null '';`
    * `copy link_type from '/home/link_type.csv' delimiter '|' null '';`
    * `copy movie_companies from '/home/movie_companies.csv' delimiter '|' null '';`
    * `copy movie_info_idx from '/home/movie_info_idx.csv' delimiter '|' null '';`
    * `copy movie_keyword from '/home/movie_keyword.csv' delimiter '|' null '';`
    * `copy movie_link from '/home/movie_link.csv' delimiter '|' null '';`
    * `copy name from '/home/name.csv' delimiter '|' null '';`
    * `copy role_type from '/home/role_type.csv' delimiter '|' null '';`
    * `copy title from '/home/title.csv' delimiter '|' null '';`
    * `copy movie_info from '/home/movie_info.csv' delimiter '|' null '';`
    * `copy person_info from '/home/person_info.csv' delimiter '|' null '';`
- `\q`
- `exit`

## 2. Setting up the PostgreSQL docker container for runtime comparisons <a name="runtime"></a>
- Configuration adjustments
    * `ALTER SYSTEM SET effective_cache_size TO '64 GB';`
    * `ALTER SYSTEM SET work_mem TO '16 GB';`
    * `ALTER SYSTEM SET statement_timeout = '5min';`
    * `ALTER SYSTEM SET enable_mergejoin = off;`
    The following configuration updates require to restart the docker container:
    * `ALTER SYSTEM SET shared_buffers TO '16 GB';`
    * `SELECT pg_reload_conf();`
- `\q`
- `exit`
- `docker stop mst_pg_docker`
- `docker start mst_pg_docker`
- `docker exec -it mst_pg_docker bash`
- `psql -h localhost -U postgres -d imdb`
- Check the updated configuration values
    * `SHOW effective_cache_size;`
    * `SHOW work_mem;`
    * `SHOW statement_timeout;`
    * `SHOW enable_mergejoin;`
    * `SHOW shared_buffers;`
- `\q`
- `exit`

Once the container is running, the provided scripts can be run outside of the container.
