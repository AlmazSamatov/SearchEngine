import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Indexer {
    private static Vocabulary vocabulary = new Vocabulary();

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "indexing");
        job.setInputFormatClass(CompleteFileInputFormat.class);
        job.setJarByClass(Indexer.class);
        readVocabulary(args[2]);
        job.setMapperClass(Indexer.IndexMap.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void readVocabulary(String pathToVocabulary) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);


        try (FSDataInputStream inputStream = fileSystem.open(new Path(pathToVocabulary))) {
            vocabulary.readFields(inputStream);
        }
    }

    public static class IndexMap extends Mapper<Text, NullWritable, DocVector, NullWritable> {
        public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            JSONObject jsonObject = null;
            try {
                jsonObject = (JSONObject) new JSONParser().parse(key.toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            assert jsonObject != null;
            int docId = Integer.valueOf(jsonObject.get("id").toString());
            String text = jsonObject.get("text").toString();
            Map<Integer, Integer> wordMap = new HashMap<>();
            Map<String, Integer> wordIds = vocabulary.getWordIds();
            StringTokenizer tokens = new StringTokenizer(text);
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
            context.write(new DocVector(docId, result), NullWritable.get());

        }
    }

//    public static class IndexReduce extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {
//        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) {
//
//        }
//    }

    public class CompleteFileInputFormat extends FileInputFormat<Text, NullWritable> {
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }

        @Override
        public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            CompleteFileRecordReader reader = new CompleteFileRecordReader();
            reader.initialize(split, context);
            return reader;
        }
    }

    public class CompleteFileRecordReader extends RecordReader<Text, NullWritable> {
        private FileSplit fileSplit;
        private Configuration conf;
        private Text key = new Text();
        private boolean processed = false;
        private JSONArray jsonArray = new JSONArray();
        private int index = 0;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) split;
            this.conf = context.getConfiguration();

            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                Text text = new Text();
                text.set(contents, 0, contents.length);
                JSONParser jsonParser = new JSONParser();
                jsonArray = (JSONArray) jsonParser.parse(text.toString());
            } catch (ParseException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(in);
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                key.set(jsonArray.get(index).toString());
                index++;
                processed = index == jsonArray.size();
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public float getProgress() throws IOException {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
        }
    }
}
