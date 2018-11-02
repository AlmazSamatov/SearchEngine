import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class VocabularyMaker {

    public static void mergeResults(String wordIdsFilePath, String idfFilePath, String pathToWrite) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        Map<String, Integer> wordIds = new HashMap<>();
        Map<Integer, Integer> idf = new HashMap<>();

        try (Scanner scanner = new Scanner(fileSystem.open(new Path(wordIdsFilePath)))) {
            while (scanner.hasNext()) {
                wordIds.put(scanner.next(), scanner.nextInt());
            }
        }

        try (Scanner scanner = new Scanner(fileSystem.open(new Path(idfFilePath)))) {
            while (scanner.hasNext()) {
                idf.put(scanner.nextInt(), scanner.nextInt());
            }
        }

        Vocabulary vocabulary = new Vocabulary(wordIds, idf);

        try (FSDataOutputStream outputStream = fileSystem.create(new Path(pathToWrite))) {
            vocabulary.write(outputStream);
        }
    }
}
