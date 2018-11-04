import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Indexer {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Start reading vocabulary...");
        Vocabulary vocabulary = Vocabulary.readVocabulary(args[args.length - 2]);
        System.out.println("Vocabulary read ended...");

        Configuration conf = new Configuration();
        conf.set("vocabulary", vocabulary.serialize());
        Job job = Job.getInstance(conf, "indexing");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(Indexer.IndexMap.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(DocVector.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < args.length - 2; i++) {
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class IndexMap extends Mapper<LongWritable, Text, DocVector, NullWritable> {

        private static Vocabulary vocabulary;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            vocabulary = Vocabulary.deserialize(context.getConfiguration().get("vocabulary"));
        }

        public void map(LongWritable offset, Text doc, Context context) throws IOException, InterruptedException {
            Document document = new Document(doc.toString());

            Map<String, Integer> wordMap = new HashMap<>();
            Map<String, Integer> wordIds = vocabulary.getWordIds();
            StringTokenizer tokens = new StringTokenizer(document.getText());

            while (tokens.hasMoreTokens()) {
                String token = tokens.nextToken().toLowerCase();
                token = token.replaceAll("[^\\w&&[^-]]", "");
                if (token.length() != 0) {
                    if (!wordMap.containsKey(token)) {
                        wordMap.put(token, 1);
                    } else {
                        wordMap.put(token, wordMap.get(token) + 1);
                    }
                }
            }

            Map<Integer, Double> result = new HashMap<>();
            Map<String, Integer> idf = vocabulary.getIdf();

            for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
                Integer wordId = wordIds.get(entry.getKey());
                result.put(wordId, (double) entry.getValue() / (double) idf.get(entry.getKey()));
            }

            context.write(new DocVector(document.getId(), result), NullWritable.get());
        }
    }
}
