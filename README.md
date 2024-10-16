# Airflow Astro Migrate

This tool provides a cli to migrate dag metadata between an airflow metastore and astronomer. 

The connection with the source metastore is done via PG client (postgress)
The connectio with Astronomer deployment is done via [Starship](https://astronomer.github.io/starship/) that provides the following endpoints:
- POST /api/starship/dag_runs  
- POST /api/starship/task_instances 



## How to use

Clone this repository and install the dependencies:

```bash
npm install
```

Once everythin is installed, you need to export the required environment variables in the terminal you will execute it:

```bash
# Astronomer environment variables
export CREDENTIALS_ASTRO_API_TOKEN=eyJhbGciOiJS...
export CREDENTIALS_ASTRO_ORG_ID=clw...
export CREDENTIALS_ASTRO_WORKSPACE_ID=clw...
export CREDENTIALS_ASTRO_DEPLOYMENT_ID=clx...
# Airflow environment variables (PostgreSQL)
export PGHOST="airflow..."
export PGPORT=5432
export PGUSER=airflow...
export PGPASSWORD="pass....."
export PGDATABASE="db....."
```

Finally run it:

```bash
npm start
```

You need to provide:


| param | description |
|---|---|
| DagId|in SQL Like format. e.g. monitor% will match all dags which dag_id starts with monitor|
| Include Paused Dags| If the paused dags are going to be considered in the migration list or not|
| Total DagRuns to migrate| For each DAG found, this is the amount of dag_runs that will be migrated. Each dag_run will also include all their tas_instances|
| Do you want to review the list| If you say Yes then a new text file will be created so you can review the DAGs list and confirm that those are all the ones you need|



### References

Starship API: https://astronomer.github.io/starship/api/#astronomer_starship.starship_api.StarshipApi.task_instances--post-apistarshiptask_instances
