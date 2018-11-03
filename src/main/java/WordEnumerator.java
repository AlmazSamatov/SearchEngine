import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.StringReader;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class WordEnumerator {
  private static int wordCounter = 0;

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      BufferedReader reader = new BufferedReader(new StringReader(value.toString()));  
      
      JSONParser parser = new JSONParser();
      String str;

      try {
        while ((str = reader.readLine()) != null) {
          JSONObject json = (JSONObject) parser.parse(str);
          String text = (String) json.get("text");
          System.out.println((String) json.get("id"));
          StringTokenizer itr = new StringTokenizer(text);
          while (itr.hasMoreTokens()) {
            String token = itr.nextToken().toLowerCase();
            token = token.replaceAll("[^\\w&&[^-]]", "");
            word.set(token);
            context.write(word, one);
          }
        }
      }
      catch (Exception e){
          e.printStackTrace();
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      result.set(wordCounter++);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordEnumerator.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    for (int i = 0; i < args.length - 1; i++) {
        MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class);
    }
    FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
