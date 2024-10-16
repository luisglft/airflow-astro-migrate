import pg from 'pg';

// export PGHOST="airflowinfra-2-rds-production-iad.cluster-c067nfzisc99.us-east-1.rds.amazonaws.com"
// export PGPORT=5432
// export PGUSER=airflow_admin
// export PGPASSWORD="wDnLeZE4B8ipTh5WI0HRc0VkaskQCtX7k4kL9Nkl1JjBgMfMwi"
// export PGDATABASE="airflowinfra2"
// export application_name="metastore-migration"

if (!process.env.PGHOST) {
    throw new Error('PGHOST is required');
}

if (!process.env.PGPORT) {
    throw new Error('PGPORT is required');
}

if (!process.env.PGUSER) {
    throw new Error('PGUSER is required');
}

if (!process.env.PGPASSWORD) {
    throw new Error('PGPASSWORD is required');
}

if (!process.env.PGDATABASE) {
    throw new Error('PGDATABASE is required');
}

export const getDagIds = async (search, paused) => {
    const { Client } = pg
    const client = new Client({
        application_name: process.env.application_name,
    });

    await client.connect();
    const includePaused = paused ? '': 'and is_paused=false';

    const res = await client.query(
        `SELECT dag_id FROM dag WHERE dag_id LIKE $1 ${includePaused} ORDER BY dag_id`,
        [search],
    );

    await client.end();

    return res.rows.map((row) => row.dag_id);
}


export const getDagRuns = async (dagId, limit) => {
    const { Client } = pg
    const client = new Client({
        application_name: process.env.application_name,
    });

    await client.connect();

    const res = await client.query(
        `SELECT * FROM dag_run WHERE dag_id = $1 ORDER BY start_date DESC LIMIT $2`,
        [dagId, limit],
    );

    await client.end();

    return res.rows;
}

export const getTaskInstances = async (dag_run) => {
    const { Client } = pg
    const client = new Client({
        application_name: 'airflow-to-astro-migration',
    });

    await client.connect();


    const res = await client.query(
        `SELECT * FROM task_instance WHERE dag_id = $1 AND run_id = $2`,
        [dag_run.dag_id, dag_run.run_id],
    );


    await client.end();

    return res.rows;
}