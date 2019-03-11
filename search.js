'use strict';

const elasticsearch = require('elasticsearch');
const fs = require('fs');
const commander = require('commander');
const pkg = require('./package.json');

let inputValue = '';
let transactionIdArray = [  ];

const checkFolder = (folderName) =>
{
    fs.readdir(folderName, (err, items) => {
        let ids = [  ];

        items.forEach((item) => 
        {
            if(item.includes(`${ selectedDay }.${ selectedMonth }.${ selectedYear }`))
            {
                let lines = fs.readFileSync(`${ folderName }/${ item }`).toString().split('.zip\n');

                for(let line of lines)
                {
                    ids.push({ term: { transactionId: line } });
                    transactionIdArray.push(line)
                }
            }
        });

        ids.filter(n => n);
        searchQuery(ids);
    });
};

const searchQuery = (should) =>
{
    const esClient = new elasticsearch.Client({
        host: `${ userInput.host }:${ userInput.port }`,
        requestTimeout: 30000
    });

    esClient.search({
        index: `*${ selectedYear }`,
        size: 10000,
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
        let ids = [];

        if (!fs.existsSync('results'))
            fs.mkdirSync('results');

        hits.forEach(hit => ids.push(hit._source.transactionId));

        const found = transactionIdArray.filter((match) => ids.indexOf(match) > -1);
        const missing = transactionIdArray.filter((match) => ids.indexOf(match) < 0);

        const transactionMatches =
        {
            query:
            [
                {
                    found: found.length
                },
                {
                    missing: missing.length
                },
                {
                    total: transactionIdArray.length
                }
            ]
        };

        const transactionIds = JSON.stringify([{ found }, { missing }, transactionMatches ], null, 4);

        fs.writeFile(`./results/${ selectedDay }-${ selectedMonth }-${ selectedYear }.json`, transactionIds, (err) => {
            if (err)
                return console.error(err);

            console.log(transactionIds);
            console.log(`\n-> Created Output file: ${ selectedDay }-${ selectedMonth }-${ selectedYear }.json`);
        });
    });
};

const userInput = commander
    .version(pkg.version)
    .description(pkg.description)
    .usage('[options] <command> [...]')
    .option('-o --host <hostname>', 'hostname [localhost]', 'localhost')
    .option('-p --port <number>', 'port number [9200]', '9200')
    .option('-d --date <date>', 'date to search [01-12-2018]', '01-12-2018');

userInput
    .command('directory <input>')
    .description('searches for matches in specified folder')
    .action((input) =>
    {
        inputValue = input;
        checkFolder(input);
    });

userInput.parse(process.argv);

if (inputValue === '')
{
    userInput.help();
    process.exit(1);
}

const [ selectedDay, selectedMonth, selectedYear ] = userInput.date.split('-');
