hdfs dfs -rm out-word-enum*
hadoop jar src/WordEnumerator.jar WordEnumerator /EnWiki/AA_wiki_00 out-word-enum
hdfs dfs -tail out-word-enum/part-r-00000
