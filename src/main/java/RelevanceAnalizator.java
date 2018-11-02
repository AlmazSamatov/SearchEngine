import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RelevanceAnalizator {

    public static class RelevanceMapper
            extends Mapper<Map<IntWritable, DoubleWritable>, List<Map<IntWritable, DoubleWritable>>, IntWritable, DoubleWritable> {

        public void map(Map<IntWritable, DoubleWritable> queryVector,
                        List<Map<IntWritable, DoubleWritable>> documents, Context context) throws IOException, InterruptedException {

            for (Map.Entry<IntWritable, DoubleWritable> entry : queryVector.entrySet()) {

                double relevance = 0;

                for (Map<IntWritable, DoubleWritable> doc : documents) {

                    if (doc.containsKey(entry.getKey())) {
                        relevance += doc.get(entry.getKey()).get() * entry.getValue().get();
                    }

                }

                context.write(entry.getKey(), new DoubleWritable(relevance));
            }

        }
    }

    public static class CustomWritableComparable implements WritableComparable<CustomWritableComparable> {
        IntWritable primaryField = new IntWritable();
        DoubleWritable secondaryField = new DoubleWritable();

        CustomWritableComparable(IntWritable key, DoubleWritable value) {
            primaryField.set(key.get());
            secondaryField.set(value.get());
        }

        @Override
        public int compareTo(CustomWritableComparable o) {
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

    public static class CustomComparator extends WritableComparator {

        protected CustomComparator() {
            super(CustomWritableComparable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CustomWritableComparable k1 = (CustomWritableComparable) a;
            CustomWritableComparable k2 = (CustomWritableComparable) b;
            return k1.compareTo(k2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(RelevanceAnalizator.class);
        job.setMapperClass(RelevanceMapper.class);
        job.setSortComparatorClass(CustomComparator.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}