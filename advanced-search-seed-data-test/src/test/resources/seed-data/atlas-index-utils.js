function getAtlasSearchIndex(collection, indexName) {
    let currentIndexes = collection.getSearchIndexes(indexName);
    if (currentIndexes.length > 0) {
        return currentIndexes[0];
    } else {
        return null;
    }
}

function dropAtlasSearchIndex(collection, indexName) {
    collection.dropSearchIndex(indexName)
}

function waitStatusReady(collection, indexName) {
    let currentIndex = getAtlasSearchIndex(collection, indexName);
    while (currentIndex != null && currentIndex.status !== 'READY') {
        sleep(300);
        currentIndex = getAtlasSearchIndex(collection, indexName);
    }
}

function waitNoIndex(collection, indexName) {
    let currentIndex = getAtlasSearchIndex(collection, indexName);
    while (currentIndex != null) {
        sleep(300);
        currentIndex = getAtlasSearchIndex(collection, indexName);
    }
}

function getIndexDefinition() {
    let rawDocument = db.getSiblingDB("tempIndex").getCollection('indexes').findOne({});
    delete rawDocument._id;
    return rawDocument;
}
function dropIndexDefinition() {
    return db.getSiblingDB("tempIndex").dropDatabase();
}

function describeIndexDefinition(indexDefinition) {
    if (indexDefinition.mappings.dynamic === false) {
        let mappingFieldNames = Object.keys(indexDefinition.mappings.fields).sort();
        return `${mappingFieldNames.length} fields: [${mappingFieldNames.join(', ')}]`;
    } else {
        return 'dynamic fields';
    }
}

function createIndex(database, collectionName, indexFileName) {
    let indexName = indexFileName.replace('.json', '').replaceAll(/\//g, '').replaceAll(/\./g, '');

    let collection = db.getSiblingDB(database).getCollection(collectionName);

    let hasCurrentIndex = getAtlasSearchIndex(collection, indexName) != null;
    if (hasCurrentIndex) {
        console.log(`Dropping existing atlasSearch index...`);
        dropAtlasSearchIndex(collection, indexName);
        waitNoIndex(collection, indexName);
    }

    let indexDefinition = getIndexDefinition();
    let indexDescription = describeIndexDefinition(indexDefinition);

    console.log(`Creating ${indexName} index in ${database}.${collectionName} index with ${indexDescription}.`);
    collection.createSearchIndex(indexName, indexDefinition);
    waitStatusReady(collection, indexName);

    dropIndexDefinition();

    let createdIndex = getAtlasSearchIndex(collection, indexName);
    let createdIndexDescription = describeIndexDefinition(createdIndex.latestDefinition);
    console.log(`Created ${indexName} index in ${database}.${collectionName} with ${createdIndexDescription}, status: ${createdIndex.status}.`);
}
