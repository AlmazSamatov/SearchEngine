import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RelevanceResults implements WritableComparable<RelevanceResults> {
    private int docId;
    private double relevance;

    public int getDocId() {
        return docId;
    }

    public double getRelevance() {
        return relevance;
    }

    public void setRelevance(double relevance) {
        this.relevance = relevance;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

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
        Gson gson = new Gson();
        dataOutput.writeChars(gson.toJson(this));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Gson gson = new Gson();
        RelevanceResults relevanceResults = gson.fromJson(dataInput.readLine(), RelevanceResults.class);
        docId = relevanceResults.docId;
        relevance = relevanceResults.relevance;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}