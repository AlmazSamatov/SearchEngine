import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This class is used in Search Engine application in order to convert string query to map containing word IDs as keys
 * and TF/IDF values as values.
 */
public class QueryVectorizer {

    private Vocabulary readVocabulary(String pathToVoc) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        Vocabulary vocabulary = new Vocabulary();

        try (FSDataInputStream inputStream = fileSystem.open(new Path(pathToVoc))) {
            vocabulary.readFields(inputStream);
        }

        return vocabulary;
    }

    /**
     * This method converts string query to map.
     *
     * @param query query
     * @return map containing word IDs present in query as keys and their TF/IDF values as values.
     */
    public Map<Integer, Double> convertQueryToVector(String query, String pathToVoc) throws IOException {
        Vocabulary vocabulary = readVocabulary(pathToVoc);

        Map<Integer, Double> wordsInQuery = new HashMap<>();

        StringTokenizer tokenizer = new StringTokenizer(query);

        // Count words and their quantity present in query:
        while (tokenizer.hasMoreTokens()) {

            String token = tokenizer.nextToken();
            token = token.toLowerCase();
            token = token.replaceAll("[^\\w&&[^-]]", "");

            if (vocabulary.getWordIds().containsKey(token)) {
                // Operate only if the word is present in wordsIDs vocabulary reached from dataset:

                int tokenID = vocabulary.getWordIds().get(token);

                if (!wordsInQuery.containsKey(tokenID)) {
                    wordsInQuery.put(tokenID, 1.0);
                } else {
                    wordsInQuery.put(tokenID, wordsInQuery.get(tokenID) + 1.0);
                }
            }
        }

        // Normalize words quantities by IDF values:
        for (Map.Entry<Integer, Double> entry : wordsInQuery.entrySet()) {
            wordsInQuery.put(entry.getKey(), entry.getValue() / vocabulary.getIdf().get(entry.getKey()));
        }

        return wordsInQuery;
    }
}
