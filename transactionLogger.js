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

const cylceIds = async (ids) =>
{
    let amount = 1000,
        counter = 0,
        query = [  ],
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
            matchesCount -= amount;
            query = [  ],
            counter = 0;
        }
    }

    return ids;
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
          ids = await cylceIds(idList);

          console.log(ids);
}

initialize();