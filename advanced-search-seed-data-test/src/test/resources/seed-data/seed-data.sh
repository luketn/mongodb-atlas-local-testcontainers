set -e

CONNECTION_STRING="mongodb://localhost:27017/?directConnection=true"

echo "$(date): Building the atlas search index..."

# iterate the database dirs in the databases directory
cd databases
for db in *; do
  if [ -d "$db" ]; then
    cd $db
    cd collections
    # iterate the collection dirs in the database dir
    for collection in *; do
      if [ -d "$collection" ]; then
        cd $collection
        cd data
        # iterate the json gz files in the collection dir
        for json in *.jsonl.gz; do
          if [ -f "${json}" ]; then
            # unzip the json file
            echo "$(date): Unzipping $(pwd)/$json"
            gunzip $json
          fi
        done
        for json in *.jsonl; do
          if [ -f "${json}" ]; then
            # import the json lines file into the database, dropping the collection first
            echo "$(date): Importing $json into $db"
            mongoimport --uri "${CONNECTION_STRING}" --drop --db $db --collection $collection $json
          fi
        done
        cd ..

        cd atlas-search-indexes
        # iterate the index json files in the collection's indexes dir
        for index in *.json; do
          if [ -f "${index}" ]; then
            # import the index json file into the tempIndex database, dropping the collection first
            echo "$(date): Importing $index into tempIndex"
            mongoimport --uri "${CONNECTION_STRING}" --drop --db tempIndex --collection indexes $index

            # make a copy of the index building script, and append the createIndex call to it
            cat ../../../../../atlas-index-utils.js > create_index.js
            echo "createIndex(\"$db\", \"$collection\", \"$index\")" >> create_index.js

            # run the script to create the index
            mongosh --file create_index.js "${CONNECTION_STRING}"
          fi
        done
        cd ..
      fi
    done
    cd ..
    cd ..
  fi
done
cd ..

echo "$(date): Finished loading seed data and building the atlas search indexes."
