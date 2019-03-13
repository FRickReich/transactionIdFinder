'use strict';

const elasticsearch = require('elasticsearch'),
      fs = require('fs'),
      commander = require('commander'),
      pkg = require('./package.json');

let queryIdArray = [  ],
    matchIds = [  ];

// Check folder for content
const checkFolder = folderName =>
{
    fs.readdir(folderName, (err, items) =>
    {
        items.forEach((item) =>
        {
            // Ignore MacOS .DS_Store file:
            if(item !== '.DS_Store')
            {
                let lines = fs.readFileSync(`${ folderName }/${ item }`).toString().split('.zip\n');
                
                for(let line of lines)
                {
                    if(/^[\s]*$/.test(line) === false)
                        queryIdArray.push(line);
                }
            }
        });

        cylceIds();
    });
};

// Cycle through ids from folder
const cylceIds = async () =>
{
    let amount = 1000,
        counter = 0,
        query = [  ],
        matchesCount = queryIdArray.length;

    for (const queryId of queryIdArray)
    {
        counter ++;
        query.push(queryId);

        if(matchesCount < amount)
            amount = matchesCount;

        if(counter === amount)
        {
            await searchQuery(query, amount);

            matchesCount -= amount;
            query = [  ];
            counter = 0;
        }
    }

    createOutput();
}

// Search for matching ids:
const searchQuery = async (ids, amount) =>
{
    const selectedYear = 2019;

    await esClient.search({
        index: `archive_invoice_tenant_yearly-c_*-${ selectedYear }`,
        size: amount,
        body: {
            query: {
                bool: {
                    filter: {
                        terms: {
                            transactionId: ids
                        }
                    }
                }
            }
        }
    }).then((resp) =>
    {
        const hits = resp.hits.hits;

        hits.forEach(hit => matchIds.push(hit._source.transactionId));
    });
}

// Compare matches to get results:
const checkForResults = () =>
{
    const missing = queryIdArray.filter((match) => matchIds.indexOf(match) < 0),
          found = queryIdArray.filter((match) => matchIds.indexOf(match) > -1);

    const query =
    {
        found: found.length,
        missing: missing.length,
        total: queryIdArray.length
    };

    return {found, missing, query};
}

// Create output:
const createOutput = () =>
{
    const results = checkForResults(),
          transactionIds = JSON.stringify(results, null, 4);

    fs.writeFile(`./results/results.json`, transactionIds, (err) =>
    {
        if (err)
            return console.error(err);

        console.log(transactionIds);
    });
}

// Set up user client:
const userInput = commander
    .version(pkg.version)
    .description(pkg.description)
    .usage('[options] <command> [...]')
    .option('-o, --host <hostname>', 'hostname [localhost]', 'localhost')
    .option('-p, --port <number>', 'port number [9200]', '9200')
    .option('-d, --directory <directory>', 'directory to search for id files', 'Files');

userInput.parse(process.argv);

// Start elastic-search client:
const esClient = new elasticsearch.Client({
    host: `${ userInput.host }:${ userInput.port }`,
    requestTimeout: 30000
});

checkFolder(userInput.directory);