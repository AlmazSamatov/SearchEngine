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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

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

    public static class ContentExtractorMapper extends Mapper<LongWritable, Text, OutputDocument, NullWritable> {

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
                document.setRelevance(results.get(document.getId()));
                context.write(new OutputDocument(document), NullWritable.get());
            }
        }
    }

    private static String serializeResults(Map<Integer, Double> m) {
        Gson gson = new Gson();
        return gson.toJson(m);
    }

    private static void sortOutput(String dir) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        List<OutputDocument> outputDocumentList = new ArrayList<>();

        FileStatus[] status = fileSystem.listStatus(new Path("hdfs://namenode:9000/user/team6/" + dir));

        for (FileStatus child : status) {
            if (child.getPath().getName().charAt(0) != '_') {
                try (FSDataInputStream inputStream = fileSystem.open(child.getPath())) {
                    while (inputStream.available() > 0) {
                        OutputDocument outputDocument = new OutputDocument();
                        outputDocument.readFields(inputStream);
                        outputDocumentList.add(outputDocument);
                    }
                }
            }
        }

        Collections.sort(outputDocumentList);

        for (OutputDocument outputDocument : outputDocumentList) {
            try (PrintWriter printWriter = new PrintWriter(new File(dir))) {
                Gson gson = new Gson();
                printWriter.println(gson.toJson(outputDocument));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Map<Integer, Double> relevanceResults = readRelevanceResults(args[args.length - 2]);

        Configuration conf = new Configuration();
        conf.set("relevanceResults", serializeResults(relevanceResults));
        Job job = Job.getInstance(conf, "content extractor");
        job.setJarByClass(ContentExtractor.class);
        job.setMapperClass(ContentExtractorMapper.class);
        job.setOutputKeyClass(OutputDocument.class);
        job.setNumReduceTasks(0);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < args.length - 2; i++) {
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        job.waitForCompletion(true);

        sortOutput(args[args.length - 1]);
    }
}
