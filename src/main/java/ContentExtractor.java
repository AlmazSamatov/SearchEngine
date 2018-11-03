import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used in Search Engine application in order to extract required documents from data set using given IDs.
 */
public class ContentExtractor {

    private static Map<Integer, Double> results = new HashMap<>();

    public static void readRelevanceResults(String pathToResults) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        Vocabulary vocabulary = new Vocabulary();

        try (FSDataInputStream inputStream = fileSystem.open(new Path(pathToResults))) {
            while (inputStream.available() > 0) {
                RelevanceResults relevanceResults = new RelevanceResults();
                relevanceResults.readFields(inputStream);
                results.put(relevanceResults.getPrimaryField().get(), relevanceResults.getSecondaryField().get());
            }
        }
    }

    public static class ContentExtractorMapper extends Mapper<LongWritable, Text, Document, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Document document = new Document(new JSONObject(value));
            Double relevance = results.get(document.getId());

            if (relevance != null) {
                context.write(document, NullWritable.get());
            }
        }
    }

    public static class ContentExtractorResultsComparator extends WritableComparator {

        protected ContentExtractorResultsComparator() {
            super(Document.class, true);
        }

        @Override
        public int compare(Object a, Object b) {
            Document d1 = (Document) a;
            Document d2 = (Document) b;
            return Double.compare(results.get(d1.getId()), results.get(d2.getId()));
        }
    }

    public static void main(String[] args) throws Exception {
        readRelevanceResults(args[0]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "content extractor");
        job.setJarByClass(ContentExtractor.class);
        job.setMapperClass(ContentExtractorMapper.class);
        job.setSortComparatorClass(ContentExtractorResultsComparator.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Document.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
