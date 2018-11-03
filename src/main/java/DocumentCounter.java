import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class DocumentCounter {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try {
                JSONObject json = new JSONObject(value.toString());
                String text = json.getString("text");
                int doc = json.getInt("id");

                StringTokenizer itr = new StringTokenizer(text);
                while (itr.hasMoreTokens()) {
                    String token = itr.nextToken().toLowerCase();
                    token = token.replaceAll("[^\\w&&[^-]]", "");
                    word.set(token);
                    IntWritable docId = new IntWritable(doc);
                    context.write(word, docId);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    public static class WordDocumentCount
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashSet<Integer> docsUsed = new HashSet<Integer>();
            int cnt = 0;
            for (IntWritable val : values) {
                int v = val.get();
                if (!docsUsed.contains(v)) {
                    cnt++;
                    docsUsed.add(v);
                }
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
        job.setReducerClass(WordDocumentCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < args.length - 1; i++) {
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
