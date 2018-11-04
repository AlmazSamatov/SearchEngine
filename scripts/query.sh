if [ -z "$1" ] 
then
    echo "Query was not provided"
    exit 1
fi
hdfs dfs -rm -r doc-ids
hadoop jar RelevanceAnalizator.jar RelevanceAnalizator indexer ~/vocabulary doc-ids $1
hdfs dfs -rm -r query-result
hadoop jar ContentExtractor.jar ContentExtractor /EnWiki/AA* doc-ids query-result
