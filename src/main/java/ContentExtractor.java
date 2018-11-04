import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used in Search Engine application in order to extract required documents from data set using given IDs.
 */
public class ContentExtractor {

    public static Map<Integer, Double> readRelevanceResults(String relevanceDir) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        Vocabulary vocabulary = new Vocabulary();

        Map<Integer, Double> results = new HashMap<>();

        FileStatus[] status = fileSystem.listStatus(new Path("hdfs://namenode:9000/user/team6/" + relevanceDir));

        for (FileStatus child : status) {
            if (child.getPath().getName().charAt(0) != '_') {
                try (FSDataInputStream inputStream = fileSystem.open(child.getPath())) {
                    while (inputStream.available() > 0) {
                        RelevanceResults relevanceResults = new RelevanceResults();
                        relevanceResults.readFields(inputStream);
                        results.put(relevanceResults.getDocId(), relevanceResults.getRelevance());
                    }
                }
            }
        }

        return results;
    }

    public static class ContentExtractorMapper extends Mapper<LongWritable, Text, Document, NullWritable> {

        private static Map<Integer, Double> results = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            results = deserializeResults(context.getConfiguration().get("relevanceResults"));
        }

        private static Map<Integer, Double> deserializeResults(String s) {
            Gson gson = new Gson();
            return gson.fromJson(s, new TypeToken<Map<Integer, Double>>() {
            }.getType());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Document document = new Document(value.toString());
            Double relevance = results.get(document.getId());

            if (relevance != null) {
                context.write(document, NullWritable.get());
            }
        }
    }

    public static class ContentExtractorReducer extends Reducer<Document, NullWritable, OutputDocument, NullWritable> {

        @Override
        protected void reduce(Document key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new OutputDocument(key), NullWritable.get());
        }
    }

    public static class ContentExtractorResultsComparator extends WritableComparator {

        protected ContentExtractorResultsComparator() {
            super(OutputDocument.class, true);
        }

        @Override
        public int compare(Object a, Object b) {
            OutputDocument d1 = (OutputDocument) a;
            OutputDocument d2 = (OutputDocument) b;
            return d1.compareTo(d2);
        }
    }

    private static String serializeResults(Map<Integer, Double> m) {
        Gson gson = new Gson();
        return gson.toJson(m);
    }

    public static void main(String[] args) throws Exception {
        Map<Integer, Double> relevanceResults = readRelevanceResults(args[args.length - 2]);

        Configuration conf = new Configuration();
        conf.set("relevanceResults", serializeResults(relevanceResults));
        Job job = Job.getInstance(conf, "content extractor");
        job.setJarByClass(ContentExtractor.class);
        job.setMapperClass(ContentExtractorMapper.class);
        job.setSortComparatorClass(ContentExtractorResultsComparator.class);
        job.setReducerClass(ContentExtractorReducer.class);
        job.setMapOutputKeyClass(Document.class);
        job.setOutputKeyClass(OutputDocument.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < args.length - 2; i++) {
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
