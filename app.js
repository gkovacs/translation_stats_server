const Koa = require('koa');
const Router = require('@koa/router');
const url = require('url');
const util = require('util');

const app = new Koa();
const router = new Router();


const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery({
  projectId: 'lilt-development',
  keyFilename: 'gcloudServiceKey-development.json',
});

const fakebigquery = require('getsecret')('fakebigquery');

const schema = {
  fields: [
    {
      name: 'insertId',
      type: 'string'
    },
    {
      name: 'eventType',
      type: 'string'
    },
    {
      name: 'dataVersion',
      type: 'integer'
    },
    {
      name: 'userId',
      type: 'string'
    },
    {
      name: 'sessionId',
      type: 'string'
    },
    {
      name: 'clientTime',
      type: 'integer'
    },
    {
      name: 'serverTime',
      type: 'integer'
    },
    {
      name: 'ip',
      type: 'string'
    },
    {
      name: 'data',
      type: 'string'
    }
  ]
};

async function listTableNames(dataset_name) { // 'liltstat_ungi'
  const table_info_list = await bigquery.dataset(dataset_name).getTables();
  const output = [];
  for (const table_info of table_info_list[0]) {
    output.push(table_info.id);
  }
  //console.log(output)
  return output;
}

let cached_table_sets = {};

async function ensureTableExists(dataset_name, table_name) {
  if (cached_table_sets[dataset_name] === undefined) {
    const cached_table_list = await listTableNames(dataset_name);
    cached_table_sets[dataset_name] = new Set();
    for (let x of cached_table_list) {
      cached_table_sets[dataset_name].add(x);
    }
  }
  if (cached_table_sets[dataset_name].has(table_name)) {
    return;
  }
  cached_table_sets[dataset_name].add(table_name);
  await bigquery.dataset(dataset_name).createTable(table_name, {schema: schema});
}

async function insertToBigQuery(dataset, table, rows) {
  //const datestr = new Date().toISOString().split('T')[0].split('-').join(''); // '20201125'
  await ensureTableExists(dataset, table);
  await bigquery.dataset(dataset).table(table).insert(rows);
}

const datasetToTableToQueue = {};

const runBigQueryBatchInsert = async () => {
  for (const dataset of Object.keys(datasetToTableToQueue)) {
    for (const [table, queue] of Object.entries(
      datasetToTableToQueue[dataset]
    )) {
      if (queue.length !== 0) {
        const itemsToInsert = queue.splice(0, queue.length); // empties queue and returns a copy of the original queue elements
        try {
          // eslint-disable-next-line no-await-in-loop
          await insertToBigQuery(dataset, table, itemsToInsert);
        } catch (err) {
          console.log(util.inspect(err, {depth: null}));
        }
      }
    }
  }
};

const addToInsertQueue = (dataset, table, row) => {
  if (datasetToTableToQueue[dataset] === undefined) {
    datasetToTableToQueue[dataset] = {};
  }
  if (datasetToTableToQueue[dataset][table] === undefined) {
    datasetToTableToQueue[dataset][table] = [];
  }
  datasetToTableToQueue[dataset][table].push(row);
};

const BIGQUERY_BATCH_INSERT_INTERVAL_MS = 5500;

const startBigQueryBatchInserter = () => {
  batchInsertionProcess = setInterval(
    runBigQueryBatchInsert,
    BIGQUERY_BATCH_INSERT_INTERVAL_MS
  );
};

startBigQueryBatchInserter();

const site_to_queue_name = {
  'unetbootin.github.io': 'liltstat_ungi',
  'habitlab.github.io': 'liltstat_hlgi',
}

router.get('/addlog', async (ctx, next) => {
  //console.log(ctx.req)
  // ctx.router available
  //console.log(ctx.url)
  //const queryObject = url.parse(ctx.url,true).query
  //console.log(queryObject)
  //console.log(ctx.request.header['x-forwarded-for']);
  const queryObject = url.parse(ctx.url,true).query;
  if (queryObject.callback === undefined || queryObject.rows === undefined || queryObject.data === undefined) {
    ctx.body = 'invalid'
    return
  }
  const ip_addr = ctx.request.header['x-forwarded-for'] || '';
  const cbname = queryObject.callback;
  //console.log(queryObject);
  const rows = JSON.parse(decodeURIComponent(queryObject.rows));

  const queue_name = site_to_queue_name[rows.site];
  if (queue_name === undefined) {
    return;
  }

  delete rows.site;
  //console.log(rows);
  const data = decodeURIComponent(queryObject.data); // unparsed json
  rows.data = data;
  rows.ip = ip_addr;
  const serverTime = Date.now();
  rows.serverTime = serverTime;
  const datestr = new Date(serverTime).toISOString().split('T')[0].split('-').join(''); // '20201125'
  if (fakebigquery) {
    console.log(queue_name)
    console.log(rows)
  } else {
    addToInsertQueue(queue_name, datestr, rows);
  }
  //console.log(rows);
  ctx.response.set("content-type", "application/javascript");
  ctx.body = `${cbname}(${JSON.stringify({success:true})})`;
  //ctx.jsonp({
  //  success: true
  //});
});

app
  .use(router.routes())
  .use(router.allowedMethods());

const PORT = process.env.PORT || 3000;
app.listen(PORT);
