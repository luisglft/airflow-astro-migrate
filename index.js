
import { getDagIds, getDagRuns, getTaskInstances } from './airflowinfra2.js';
import { saveDagRuns, saveTaskInstancesByBatch } from './astro.js';
import { input, confirm, number } from '@inquirer/prompts';
import fs from 'fs';
import cliProgress from 'cli-progress'

const filenameDone = `done_dag_runs.txt`;
const waitSecBetweenRequests = 1;

if (!fs.existsSync(filenameDone)) {
    fs.writeFileSync(filenameDone, '');
}

function isDone(dagId, runId){
    const done = fs.readFileSync(filenameDone, 'utf8');
    return done.includes(`${dagId}${runId}`);
}

const dag_id_search = await input({ message: 'DagId (SQL LIKE), e.g. monitor_% ', required: true });
const includePaused = await confirm({
    message: 'Include paused DAGs?',
    initial: false,
    name: 'includePaused',
});
const totalDagRuns = await number({ message: 'Total DAG_RUNs to migrate:' });
const dagIds = await getDagIds(dag_id_search, includePaused);




console.log('Total DAGs found :', dagIds.length);

const reviewDags = await confirm({
    message: 'Do you want to review the DAGs list?',
    initial: false,
    name: 'reviewDags',
});


if(reviewDags){
    const filename = `dag_ids_${dag_id_search}.txt`;
    console.log(`Saving DAGs list to file "dag_ids_${dag_id_search}.txt" for review`);

    fs.writeFileSync(filename, dagIds.join('\n'));

    console.log('Please review the file and come back to continue...');

    const canContinue = await confirm({
        message: 'Do you wish to continue?',
        initial: false,
        name: 'canContinue',
    });
    if(!canContinue){
        console.log('Exiting...');
        process.exit(0);
    }
}



const multibar = new cliProgress.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
    format: ' {bar} | {label} | {value}/{total}[{percentage}%] - {tag}',
}, cliProgress.Presets.shades_grey);







const dagsBar = multibar.create(dagIds.length, 0, {label: 'DAGs processed', tag: 'dag_id:'+dagIds[0]});
const dagRunsBar = multibar.create(0, 0, {label: 'DagRuns processed'});
const taskInstancesBar = multibar.create(0, 0, {label: 'TaskInstances processed'});

for(const dagId of dagIds){

    const dagRuns = await getDagRuns(dagId, totalDagRuns);
    dagRunsBar.update(0);
    dagRunsBar.setTotal(dagRuns.length);

    for(const dagRun of dagRuns){
        if (isDone(dagRun.dag_id, dagRun.run_id)){
            dagRunsBar.increment(1, {tag: 'run_id:'+ dagRun.run_id});
            continue;
        }
        // saveDagRuns([dagRun]);
        await new Promise((resolve) => setTimeout(resolve, waitSecBetweenRequests*1000));
        const taskInstances = await getTaskInstances(dagRun);
        taskInstancesBar.update(0, {tag: ''});
        taskInstancesBar.setTotal(taskInstances.length);
        // await saveTaskInstancesByBatch(taskInstances, taskInstancesBar);
        dagRunsBar.increment(1, {tag: 'run_id:'+ dagRun.run_id});
        // Idempotency if running the script again in the same environment
        fs.appendFileSync(filenameDone, `${dagRun.dag_id}${dagRun.run_id}\n`);
    }
    // await new Promise((resolve) => setTimeout(resolve, waitSecBetweenRequests*1000));
    dagsBar.increment(1, {tag: 'dag_id:'+ dagId});
}




multibar.stop();

