hdfs dfs -rm -r word-ids
hadoop jar WordEnumerator.jar WordEnumerator /EnWiki/AA* word-ids
hdfs dfs -rm -r idf
hadoop jar DocumentCounter.jar DocumentCounter /EnWiki/AA* idf

hadoop jar VocabularyMaker.jar VocabularyMaker word-ids idf ~/vocabulary
hdfs dfs -rm -r indexer
hadoop jar Indexer.jar Indexer /EnWiki/AA* ~/vocabulary indexer
