import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class RelevanceAnalizator {

    public static class RelevanceMapper extends Mapper<LongWritable, Text, RelevanceResults, NullWritable> {

        private static Map<Integer, Double> queryVector;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            queryVector = QueryVectorizer.deserialize(context.getConfiguration().get("queryVector"));
        }

        public void map(LongWritable offset, Text doc, Context context) throws IOException, InterruptedException {
            DocVector docVector = new DocVector(doc.toString());

            for (Map.Entry<Integer, Double> entry : queryVector.entrySet()) {

                double relevance = 0;

                if (docVector.getVector().containsKey(entry.getKey())) {
                    relevance += docVector.getVector().get(entry.getKey()) * entry.getValue();
                    context.write(new RelevanceResults(entry.getKey(), relevance), NullWritable.get());
                }

            }

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
        StringBuilder query = new StringBuilder();
        for (int i = 3; i < args.length; i++){
            query.append(args[i]);
        }
        Map<Integer, Double> queryVector = QueryVectorizer.convertQueryToVector(query.toString(), args[1]);
        System.out.println(queryVector);

        Configuration conf = new Configuration();
        conf.set("queryVector", QueryVectorizer.serialize(queryVector));
        Job job = Job.getInstance(conf, "relevance analizator");
        job.setJarByClass(RelevanceAnalizator.class);
        job.setMapperClass(RelevanceMapper.class);
        job.setSortComparatorClass(RelevanceResultsComparator.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(RelevanceResults.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}