'use strict';

const elasticsearch = require('elasticsearch');
const fs = require('fs');
const commander = require('commander');
const pkg = require('./package.json');

let inputValue = '';
let matchesFound = 0;
let matchesNotFound = 0;
let matches = 0;
let found = [];
let missing = [];

const checkFolder = (folderName) =>
{
    fs.readdir(folderName, (err, items) =>
    {
        var ids = [];

        for (var i=0; i<items.length; i++)
        {

            if(items[i].includes(`${selectedDay}${selectedMonth}${selectedYear}`))
            {
                var lines = fs.readFileSync(`${folderName}/${items[i]}`).toString().split('.zip\n');

                for(let line in lines)
                {
                    if(lines[line] !== "")
                    {
                        ids.push({ term: { transactionId: lines[line] }});
                    }
                }
            }
        }

        ids.filter(n => n);
        searchQuery(ids);
    });
};

const searchQuery = (should) =>
{
    const esClient = new elasticsearch.Client(
    {
        host: `${userInput.host}:${userInput.port}`,
        requestTimeout: 30000
    });

    esClient.search(
    {
        index: `*${selectedYear}`,
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

        if(hits.length === 0)
        {
            console.log(JSON.stringify({"ERROR": `no entries found`}, null, 4));
        }
        else
        {
            for(let i = 0; i < should.length; i++)
            {
                matches++;

                if(hits[i])
                {
                    matchesFound++;

                    found.push(hits[i]._source.transactionId);
                }
                else
                {
                    matchesNotFound++;

                    missing.push(should[i].term.transactionId);
                }
            }

            const transactionMatches = { "query": [{"found": matchesFound}, {"missing": matchesNotFound}, {"total": matches} ]};
            const transactionIds = [{found}, {missing}, transactionMatches];

            fs.writeFile(`./Results/${selectedDay}-${selectedMonth}-${selectedYear}.json`, JSON.stringify(transactionIds, null, 4), (err) =>
            {
                if (err) {
                    console.error(err);
                    return;
                };
                console.log(JSON.stringify(transactionIds, null, 4));
                console.log(`\n-> Created Output file: ${selectedDay}-${selectedMonth}-${selectedYear}.json`);
            });
        }
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

const selectedDate = userInput.date.split("-");
const selectedDay = selectedDate[0];
const selectedMonth = selectedDate[1];
const selectedYear = selectedDate[2];






