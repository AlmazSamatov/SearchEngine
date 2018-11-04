import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class VocabularyMaker {

    public static void mergeResults(String wordIdsDir, String idfDir, String pathToWrite) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        Map<String, Integer> wordIds = new HashMap<>();
        Map<Integer, Integer> idf = new HashMap<>();

        FileStatus[] fileStatus = fileSystem.listStatus(new Path("hdfs://namenode:9000/user/team6/" + wordIdsDir));
        for (FileStatus status : fileStatus) {
            try (Scanner scanner = new Scanner(fileSystem.open(status.getPath()))) {
                while (scanner.hasNext()) {
                    wordIds.put(scanner.next(), scanner.nextInt());
                }
            }
        }

        fileStatus = fileSystem.listStatus(new Path("hdfs://namenode:9000/user/team6/" + idfDir));
        for (FileStatus status : fileStatus) {
            try (Scanner scanner = new Scanner(fileSystem.open(status.getPath()))) {
                while (scanner.hasNext()) {
                    idf.put(scanner.nextInt(), scanner.nextInt());
                }
            }
        }

        Vocabulary vocabulary = new Vocabulary(wordIds, idf);

        try (FSDataOutputStream outputStream = fileSystem.create(new Path("hdfs://namenode:9000/user/team6/" + vocabulary))) {
            vocabulary.write(outputStream);
        }
    }

    public static void main(String[] args) throws IOException {
        mergeResults(args[0], args[1], args[2]);
    }
}
