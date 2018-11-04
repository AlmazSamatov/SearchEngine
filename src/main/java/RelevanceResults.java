import com.google.gson.Gson;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RelevanceResults implements WritableComparable<RelevanceResults> {
    private IntWritable docId = new IntWritable();
    private DoubleWritable relevance = new DoubleWritable();

    public IntWritable getDocId() {
        return docId;
    }

    public void setDocId(IntWritable docId) {
        this.docId = docId;
    }

    public DoubleWritable getRelevance() {
        return relevance;
    }

    public void setRelevance(DoubleWritable relevance) {
        this.relevance = relevance;
    }

    RelevanceResults() {
    }

    RelevanceResults(int key, double value) {
        docId.set(key);
        relevance.set(value);
    }

    @Override
    public int compareTo(RelevanceResults o) {
        return Double.compare(o.relevance.get(), relevance.get());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        docId.write(dataOutput);
        relevance.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        docId.readFields(dataInput);
        relevance.readFields(dataInput);
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}