import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Indexer {
    private static Vocabulary vocabulary = new Vocabulary();

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Vocabulary.readVocabulary(args[2]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "indexing");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(Indexer.IndexMap.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class IndexMap extends Mapper<LongWritable, Text, DocVector, NullWritable> {
        public void map(LongWritable offset, Text doc, Context context) throws IOException, InterruptedException {
            Document document = new Document(new JSONObject(doc.toString()));

            Map<Integer, Integer> wordMap = new HashMap<>();
            Map<String, Integer> wordIds = vocabulary.getWordIds();
            StringTokenizer tokens = new StringTokenizer(document.getText());

            while (tokens.hasMoreTokens()) {
                String token = tokens.nextToken().toLowerCase();
                token = token.replaceAll("[^\\w&&[^-]]", "");
                Integer wordId = wordIds.get(token);
                if (!wordMap.containsKey(wordId)) {
                    wordMap.put(wordId, 1);
                } else {
                    wordMap.put(wordId, wordMap.get(wordId) + 1);
                }
            }

            Map<Integer, Double> result = new HashMap<>();
            for (Map.Entry<Integer, Integer> entry : vocabulary.getIdf().entrySet()) {
                result.put(entry.getKey(), (double) (wordMap.get(entry.getKey()) / entry.getValue()));
            }
            context.write(new DocVector(document.getId(), result), NullWritable.get());

        }
    }
}
