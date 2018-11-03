import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class RelevanceAnalizator {

    private static Map<Integer, Double> queryVector;

    public static class RelevanceMapper extends Mapper<DocVector, NullWritable, RelevanceResults, NullWritable> {

        public void map(DocVector docVector, NullWritable n, Context context) throws IOException, InterruptedException {

            for (Map.Entry<Integer, Double> entry : queryVector.entrySet()) {

                double relevance = 0;

                if (docVector.getVector().containsKey(entry.getKey())) {
                    relevance += docVector.getVector().get(entry.getKey()) * entry.getValue();
                }

                context.write(new RelevanceResults(entry.getKey(), relevance), NullWritable.get());
            }

        }
    }

    public class CustomInputFormat extends FileInputFormat<DocVector, NullWritable> {

        @Override
        public RecordReader<DocVector, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
            CustomRecordReader customRecordReader = new CustomRecordReader();
            customRecordReader.initialize(inputSplit, taskAttemptContext);
            return customRecordReader;
        }
    }

    public class CustomRecordReader extends RecordReader<DocVector, NullWritable> {

        private DocVector currentKey = new DocVector();
        private FSDataInputStream inputStream;
        private int initialSize = 0;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
            FileSplit split = (FileSplit) inputSplit;
            Configuration job = taskAttemptContext.getConfiguration();

            final Path file = split.getPath();
            FileSystem fs = file.getFileSystem(job);
            inputStream = fs.open(split.getPath());

            initialSize = inputStream.available();
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (inputStream.available() > 0) {
                currentKey.readFields(inputStream);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public DocVector getCurrentKey() {
            return currentKey;
        }

        @Override
        public NullWritable getCurrentValue() {
            return NullWritable.get();
        }

        @Override
        public float getProgress() throws IOException {
            return (initialSize - inputStream.available()) / initialSize;
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
        }
    }

    public static class RelevanceResults implements WritableComparable<RelevanceResults> {
        IntWritable primaryField = new IntWritable();
        DoubleWritable secondaryField = new DoubleWritable();

        RelevanceResults(int key, double value) {
            primaryField.set(key);
            secondaryField.set(value);
        }

        @Override
        public int compareTo(RelevanceResults o) {
            return Double.compare(secondaryField.get(), o.secondaryField.get());
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            primaryField.write(dataOutput);
            secondaryField.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            primaryField.readFields(dataInput);
            secondaryField.readFields(dataInput);
        }
    }

    public static class RelevanceResultsComparator extends WritableComparator {

        protected RelevanceResultsComparator() {
            super(RelevanceResults.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            RelevanceResults k1 = (RelevanceResults) a;
            RelevanceResults k2 = (RelevanceResults) b;
            return k1.compareTo(k2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        QueryVectorizer queryVectorizer = new QueryVectorizer();
        queryVector = queryVectorizer.convertQueryToVector(args[0], args[3]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "relevance analizator");
        job.setJarByClass(RelevanceAnalizator.class);
        job.setInputFormatClass(CustomInputFormat.class);
        job.setMapperClass(RelevanceMapper.class);
        job.setSortComparatorClass(RelevanceResultsComparator.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}