'use strict';

const elasticsearch = require('elasticsearch');
const fs = require('fs');
const commander = require('commander');
const pkg = require('./package.json');

let queryIdArray = [  ],
    matchIds     = [  ],
    found        = [  ],
    missing      = [  ];

// - CHECK FOLDER FOR FILES
// - CYCLE FILES
// - ADD LINES TO ARRAY
// - SEARCH FOR MATCHES
// - ADD MATCHES TO ARRAY
// - COMPARE MATCHES AND QUERY LINES
// - OUTPUT

const checkFolder = (folderName) =>
{
    fs.readdir(folderName, (err, items) => 
    {
        items.forEach((item) =>
        {
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

const cylceIds = async () =>
{
    let amount       = 500,
        counter      = 0,
        query        = [  ],
        matchesCount = queryIdArray.length;

    for (const queryId of queryIdArray)
    {
        counter ++;
        query.push({ term: { transactionId: queryId } });

        if(matchesCount < amount)
            amount = matchesCount;

        if(counter === amount)
        {
            await searchQuery(query);
            
            matchesCount -= amount;
            query = [  ];
            counter = 0;
        }
    }

    outputResult();
}

const searchQuery = async (ids) =>
{
    const selectedYear = 2019;

    await esClient.search({
        index: `*${ selectedYear }`,
        body: {
            query: {
                bool: {
                    must: [
                        {
                            bool: {
                                should: ids
                            }
                        }
                    ]
                }
            }
        }
    }).then((resp) =>
    {
        const hits = resp.hits.hits;

        hits.forEach(hit => matchIds.push(hit._source.transactionId));
    });
}

const outputResult = () =>
{
    missing = queryIdArray.filter((match) => matchIds.indexOf(match) < 0);
    found = queryIdArray.filter((match) => matchIds.indexOf(match) > -1);

    const query =
    {
        found: found.length,
        missing: missing.length,
        total: queryIdArray.length
    };

    const transactionIds = JSON.stringify({ found, missing, query}, null, 4);

    fs.writeFile(`./results/results.json`, transactionIds, (err) => {
        if (err)
            return console.error(err);

        console.log(transactionIds);
    });
}

const userInput = commander
    .version(pkg.version)
    .description(pkg.description)
    .usage('[options] <command> [...]')
    .option('-o, --host <hostname>', 'hostname [localhost]', 'localhost')
    .option('-p, --port <number>', 'port number [9200]', '9200')
    .option('-d, --directory <directory>', 'directory to search for id files', 'Files');

userInput.parse(process.argv);

const esClient = new elasticsearch.Client({
    host: `${ userInput.host }:${ userInput.port }`,
    requestTimeout: 30000
});

checkFolder(userInput.directory);
