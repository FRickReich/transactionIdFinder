'use strict';

const elasticsearch = require('elasticsearch'),
      { promisify } = require('util'),
      fs = require('fs'),
      commander = require('commander'),
      pkg = require('./package.json');

const readdir = promisify(fs.readdir);

// Check folder for files and return all files, excluding OSX's ".DS_Store":
const checkFolder = async (folderName) =>
{
    let  names = [  ];

    try
    {
        names = await readdir(folderName);
    }
    catch (e)
    {
        console.log('e', e);
    }
    finally
    {
        return names.filter((item) => 
        {
            // Ignore MacOS .DS_Store file:
            return item != '.DS_Store';
        });
    }
}

// Return an array of Id's from files and filter out empty lines:
const createIdArray = (files) =>
{
    let transactionIdList = [  ];

    try 
    {
        files.forEach((file) =>
        {
            let data = fs.readFileSync(`${ userInput.directory }/${ file }`),
                lines = data.toString().split('.zip\n')

            for(let line of lines)
            {
                transactionIdList.push(line);
            }
        });
    }
    catch (e)
    {
        console.log('e', e);
    }
    finally
    {
        return transactionIdList.filter((item) =>
        {
            return !/^[\s]*$/.test(item);
        });
    }
}

// Cycle through ids from array and search for them on database:
const cylceIds = async (ids) =>
{
    let amount = 1000,
        counter = 0,
        query = [  ],
        foundMatches = [  ],
        matchesCount = ids.length;

    const selectedYear = userInput.year;

    for (const id of ids)
    {
        counter ++;
        query.push(id);

        if(matchesCount < amount)
            amount = matchesCount;

        if(counter === amount)
        {
            await esClient.search({
                index: `archive_invoice_tenant_yearly-c_*-${ selectedYear }`,
                size: amount,
                body: {
                    query: {
                        bool: {
                            filter: {
                                terms: {
                                    transactionId: query
                                }
                            }
                        }
                    }
                }
            }).then((resp) =>
            {
                const hits = resp.hits.hits;
        
                hits.forEach(hit => foundMatches.push(hit._source.transactionId));
            });

            matchesCount -= amount;
            query = [  ],
            counter = 0;
        }
    }

    return foundMatches;
}

const checkResults = (ids, list) =>
{
    const missing = list.filter((match) => ids.indexOf(match) < 0),
          found = list.filter((match) => ids.indexOf(match) > -1);

    const query =
    {
        found: found.length,
        missing: missing.length,
        total: list.length
    };

    return {found, missing, query};
}

const createOutput = (matches) =>
{
    const transactionIds = JSON.stringify(matches, null, 4);

    if (!fs.existsSync('results'))
        fs.mkdirSync('results');

    fs.writeFile(`./results/results.json`, transactionIds, (err) =>
    {
        if (err)
            return console.error(err);

        console.log(transactionIds);
    });
}

// Set up options for user input:
const userInput = commander
    .version(pkg.version)
    .description(pkg.description)
    .usage('[options] <command> [...]')
    .option('-o, --host <hostname>', 'hostname [localhost]', 'localhost')
    .option('-p, --port <number>', 'port number [9200]', '9200')
    .option('-d, --directory <directory>', 'directory to search for id files', 'FILES_TO_CHECK')
    .option('-y, --year <year>', 'year to search for', '2019');

userInput.parse(process.argv);

// Set up and start elastic-search client:
const esClient = new elasticsearch.Client({
    host: `${ userInput.host }:${ userInput.port }`,
    requestTimeout: 30000
});

// Main application:
const initialize = async () =>
{
    const fileNames = await checkFolder(userInput.directory),
          idList = await createIdArray(fileNames),
          results = await cylceIds(idList),
          matches = await checkResults(results, idList);
          
    createOutput(matches);
}

initialize();