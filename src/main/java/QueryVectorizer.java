import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

/**
 * This class is used in Search Engine application in order to convert string query to map containing word IDs as keys
 * and TF/IDF values as values.
 */
public class QueryVectorizer {

    public static Map<Integer, Double> deserialize(String s) {
        Gson gson = new Gson();
        return gson.fromJson(s, new TypeToken<Map<Integer, Double>>() {}.getType());
    }

    public static String serialize(Map<Integer, Double> m) {
        Gson gson = new Gson();
        return gson.toJson(m);
    }

    /**
     * This method converts string query to map.
     *
     * @param query query
     * @return map containing word IDs present in query as keys and their TF/IDF values as values.
     */
    public static Map<Integer, Double> convertQueryToVector(String query, String vocDir) throws IOException {
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

        Map<Integer, String> wordIdsInverse = vocabulary.getWordIds()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        Map<Integer, Double> res = new HashMap<>();

        // Normalize words quantities by IDF values:
        for (Map.Entry<Integer, Double> entry : wordsInQuery.entrySet()) {
            res.put(entry.getKey(), entry.getValue() / (double) vocabulary.getIdf().get(wordIdsInverse.get(entry.getKey())));
        }

        return res;
    }
}
