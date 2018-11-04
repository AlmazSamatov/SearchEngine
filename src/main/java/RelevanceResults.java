import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RelevanceResults implements WritableComparable<RelevanceResults> {
    private int docId;
    private double relevance;

    RelevanceResults() {
    }

    RelevanceResults(int key, double value) {
        docId = key;
        relevance = value;
    }

    @Override
    public int compareTo(RelevanceResults o) {
        return Double.compare(o.relevance, relevance);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(docId);
        dataOutput.writeDouble(relevance);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        docId = dataInput.readInt();
        relevance = dataInput.readDouble();
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}