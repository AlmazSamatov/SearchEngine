import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This class is used in Search Engine application in order to convert string query to map containing word IDs as keys
 * and TF/IDF values as values.
 */
public class QueryVectorizer {

    /**
     * This method converts string query to map.
     *
     * @param query query
     * @return map containing word IDs present in query as keys and their TF/IDF values as values.
     */
    public Map<Integer, Double> convertQueryToVector(String query, String vocDir) throws IOException {
        Vocabulary vocabulary = Vocabulary.readVocabulary(vocDir);

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
