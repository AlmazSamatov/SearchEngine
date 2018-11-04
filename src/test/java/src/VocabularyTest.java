package src;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VocabularyTest {

    @org.junit.Test
    public void checkVocabularyWriteRead() throws IOException {
        Vocabulary vocabulary = new Vocabulary();
        Map<String, Integer> m = new HashMap<>();
        m.put("asdas", 123);
        m.put("asda", 124);

        vocabulary.setIdf(m);
        vocabulary.setWordIds(m);

        Vocabulary.writeVocabularyToFile(vocabulary, "vocabulary");

        Vocabulary vocabulary1 = Vocabulary.readVocabulary("vocabulary");

        assert vocabulary.getIdf().equals(vocabulary1.getIdf());
    }
}
