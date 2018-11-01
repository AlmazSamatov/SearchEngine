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
     * @param query query
     * @param wordsIDs map containing words strings as keys and their IDs as values.
     * @param IDFVocabulary map containing word IDs as keys and their IDF values as values.
     * @return map containing word IDs present in query as keys and their TF/IDF values as values.
     */
    public static Map<Integer, Double> convertQueryToVector(String query, Map<String, Integer> wordsIDs, Map<Integer, Integer> IDFVocabulary) {
        Map<Integer, Double> wordsInQuery = new HashMap<>();

        StringTokenizer tokenizer = new StringTokenizer(query);

        // Count words and their quantity present in query:
        while (tokenizer.hasMoreTokens()) {

            String token = tokenizer.nextToken();
            token = token.toLowerCase();
            token = token.replaceAll("[^\\w&&[^-]]", "");

            if (wordsIDs.containsKey(token)) {
                // Operate only if the word is present in wordsIDs vocabulary reached from dataset:

                int tokenID = wordsIDs.get(token);

                if (!wordsInQuery.containsKey(tokenID)) {
                    wordsInQuery.put(tokenID, 1.0);
                } else {
                    wordsInQuery.put(tokenID, wordsInQuery.get(tokenID) + 1.0);
                }
            }
        }

        // Normalize words quantities by IDF values:
        for (Map.Entry<Integer, Double> entry : wordsInQuery.entrySet()) {
            wordsInQuery.put(entry.getKey(), entry.getValue() / IDFVocabulary.get(entry.getKey()));
        }

        return wordsInQuery;
    }

    public static void main(String[] args) {
        String query = "the population";
        Map<String, Integer> wordsIDs = new HashMap<>();
        Map<Integer, Integer> IDFVocabulary = new HashMap<>();

        wordsIDs.put("population", 22);
        wordsIDs.put("the", 24);

        IDFVocabulary.put(22, 2);
        IDFVocabulary.put(24, 3);

        Map<Integer, Double> vector = convertQueryToVector(query, wordsIDs, IDFVocabulary);
        for (Map.Entry<Integer, Double> entry : vector.entrySet()) {
            System.out.println("(" + entry.getKey() + ": " + entry.getValue() + ")");
        }
    }
}
