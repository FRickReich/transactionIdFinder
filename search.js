'use strict';

const elasticsearch = require('elasticsearch');
const fs = require('fs');
const commander = require('commander');
const pkg = require('./package.json');

let queryIdArray = [  ];
let matchIds = [  ];
let found = [  ];
let missing = [  ];

const checkFolder = async (folderName) =>
{
    await fs.readdir(folderName, (err, items) => {

        let ids = [  ];

        items.forEach((item) =>
        {
            if(item !== '.DS_Store')
            {
                let lines = fs.readFileSync(`${ folderName }/${ item }`).toString().split('.zip\n');

                for(let line of lines)
                {
                    if(/^[\s]*$/.test(line) === false)
                    {
                        queryIdArray.push(line);
                    }
                }
            }
        });

        cylceIds();
    });
};

const cylceIds = async () =>
{
    const amount = 1000;
    let counter = 0;
    let query = [  ];

    for (const queryId of queryIdArray)
    {
        if(counter <= amount)
        {
            query.push({ term: { transactionId: queryId } });

            if(counter === amount)
            {
                await searchQuery(query);
                query = [  ];
                counter = 0;
            }
        }

        counter += 1;
    }
    
    outputResult(matchIds);
}

const searchQuery = async (should) =>
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
                                should
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

const outputResult = async (match) =>
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

    await fs.writeFile(`./results/results.json`, transactionIds, (err) => {
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
