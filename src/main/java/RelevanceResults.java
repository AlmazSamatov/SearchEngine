import com.google.gson.Gson;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RelevanceResults implements WritableComparable<RelevanceResults> {
    private IntWritable primaryField = new IntWritable();
    private DoubleWritable secondaryField = new DoubleWritable();

    public IntWritable getPrimaryField() {
        return primaryField;
    }

    public void setPrimaryField(IntWritable primaryField) {
        this.primaryField = primaryField;
    }

    public DoubleWritable getSecondaryField() {
        return secondaryField;
    }

    public void setSecondaryField(DoubleWritable secondaryField) {
        this.secondaryField = secondaryField;
    }

    RelevanceResults() {
    }

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

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}