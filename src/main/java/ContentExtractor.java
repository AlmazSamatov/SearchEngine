import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;

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

    public static class ContentExtractorMapper extends Mapper<Document, NullWritable, Document, NullWritable> {

        @Override
        protected void map(Document key, NullWritable value, Context context) throws IOException, InterruptedException {
            Double relevance = results.get(key.getId());
            if (relevance != null) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public class CustomInputFormat extends FileInputFormat<Document, NullWritable> {

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public RecordReader<Document, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
            CustomRecordReader customRecordReader = new CustomRecordReader();
            customRecordReader.initialize(inputSplit, taskAttemptContext);
            return customRecordReader;
        }
    }

    public class CustomRecordReader extends RecordReader<Document, NullWritable> {

        private Document currentKey = new Document();
        private JSONArray jsonArray;
        private int currentIndex = 0;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
            FileSplit split = (FileSplit) inputSplit;
            Configuration job = taskAttemptContext.getConfiguration();

            final Path file = split.getPath();
            FileSystem fs = file.getFileSystem(job);
            FSDataInputStream inputStream = fs.open(split.getPath());
            byte[] bs = new byte[inputStream.available()];
            inputStream.read(bs);
            String s = new String(bs);
            jsonArray = new JSONArray(s);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (currentIndex + 1 < jsonArray.length()) {
                currentKey = new Document(jsonArray.getJSONObject(currentIndex));
                currentIndex++;
                return true;
            }
            return false;
        }

        @Override
        public Document getCurrentKey() {
            return currentKey;
        }

        @Override
        public NullWritable getCurrentValue() {
            return NullWritable.get();
        }

        @Override
        public float getProgress() throws IOException {
            return currentIndex / jsonArray.length();
        }

        @Override
        public void close() throws IOException {
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
        job.setInputFormatClass(CustomInputFormat.class);
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
