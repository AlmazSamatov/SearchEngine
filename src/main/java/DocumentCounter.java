import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;

public class DocumentCounter {
    public static HashMap<String, Integer> docIds = new HashMap<String, Integer>();
    public static int docCnt = 0;

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private IntWritable fileId = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            Integer docId = docIds.get(fileName);
            if (docId == null) {
                docIds.put(fileName, docCnt);
                fileId.set(docCnt++);
            } else {
                fileId.set(docId);
            }

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase();
                token = token.replaceAll("[^\\w&&[^-]]", "");
                
                word.set(token);
                context.write(word, fileId);
            }
        }
    }

    public static class WordDocumentCount
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // TODO: understand why everything works without set
            //HashSet<Integer> docsUsed = new HashSet<Integer>();
            int cnt = 0;
            for (IntWritable val: values) {
                cnt++;
                /*int v = val.get();
                if (!docsUsed.contains(v)) {
                    cnt++;
                    docsUsed.add(v);
                }*/
            }
            result.set(cnt);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "document count");
        job.setJarByClass(DocumentCounter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(WordDocumentCount.class);
        job.setReducerClass(WordDocumentCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);//IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
