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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used in Search Engine application in order to extract required documents from data set using given IDs.
 */
public class ContentExtractor {

    private static Map<Integer, Double> results = new HashMap<>();

    public static void readRelevanceResults(String vocDir) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        Vocabulary vocabulary = new Vocabulary();

        File dir = new File(vocDir);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {
                if (child.getName().charAt(0) != '_') {
                    try (FSDataInputStream inputStream = fileSystem.open(new Path(child.getPath().toString()))) {
                        while (inputStream.available() > 0) {
                            RelevanceResults relevanceResults = new RelevanceResults();
                            relevanceResults.readFields(inputStream);
                            results.put(relevanceResults.getPrimaryField().get(), relevanceResults.getSecondaryField().get());
                        }
                    }
                }
            }
        }


    }

    public static class ContentExtractorMapper extends Mapper<LongWritable, Text, Document, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Document document = new Document(value.toString());
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
        readRelevanceResults(args[args.length - 1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "content extractor");
        job.setJarByClass(ContentExtractor.class);
        job.setMapperClass(ContentExtractorMapper.class);
        job.setSortComparatorClass(ContentExtractorResultsComparator.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Document.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < args.length - 2; i++) {
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
