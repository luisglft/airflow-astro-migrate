import got from "got";
import fs from 'fs';

const token = process.env.CREDENTIALS_ASTRO_API_TOKEN;
const orgId = process.env.CREDENTIALS_ASTRO_ORG_ID;
const workspaceId = process.env.CREDENTIALS_ASTRO_WORKSPACE_ID;
const deploymentId = process.env.CREDENTIALS_ASTRO_DEPLOYMENT_ID;

if (!token) {
    throw new Error('CREDENTIALS_ASTRO_API_TOKEN is required');
}

if (!orgId) {
    throw new Error('CREDENTIALS_ASTRO_ORG_ID is required');
}

if (!workspaceId) {
    throw new Error('CREDENTIALS_ASTRO_WORKSPACE_ID is required');
}

if (!deploymentId) {
    throw new Error('CREDENTIALS_ASTRO_DEPLOYMENT_ID is required');
}

export const saveDagRuns = async (dagRuns) => {
    const url = `https://${orgId}.astronomer.run/d${deploymentId.substr(17)}/api/starship/dag_runs?dag_id=${dagRuns[0].dag_id}`;
    const headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': `https://${orgId}.astronomer.run`,
        'Pragma': 'no-cache',
        'Referer': `https://${orgId}.astronomer.run`,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Starship-Proxy-Token': token,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    };

    return await got.post(url, {
        headers,
        json: {
            dag_runs: dagRuns,
        },
    });

};

const saveTaskInstances = async (taskInstances) => {
    const url = `https://${orgId}.astronomer.run/d${deploymentId.substr(17)}/api/starship/task_instances?dag_id=${taskInstances[0].dag_id}`;
    const headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': `https://${orgId}.astronomer.run`,
        'Pragma': 'no-cache',
        'Referer': `https://${orgId}.astronomer.run`,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Starship-Proxy-Token': token,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    };

    return await got.post(url, {
        headers,
        json: {
            task_instances: taskInstances,
        },
    });

}

export const saveTaskInstancesByBatch = async (taskInstances, progrssBar) => {
    const batches = [];
    const batchSize = 5;
    for (let i = 0; i < taskInstances.length; i += batchSize) {
        batches.push(taskInstances.slice(i, i + batchSize));
    }

    for (const batch of batches) {
        await saveTaskInstances(batch);
        await new Promise((resolve) => setTimeout(resolve, 1000));
        progrssBar.increment(batch.length, { tag: batch.length + ' tasks' });
    }
}
    