import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class DocVector implements WritableComparable<DocVector> {
    private int docId;
    private Map<Integer, Double> vector;

    DocVector() {
    }

    DocVector(String serializedDocVector) {
        Gson gson = new Gson();
        DocVector docVector = gson.fromJson(serializedDocVector, DocVector.class);
        docId = docVector.docId;
        vector = docVector.vector;
    }

    DocVector(int id, Map<Integer, Double> wordMap) {
        docId = id;
        vector = wordMap;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Gson gson = new Gson();
        String serializedDocVector = gson.toJson(this);
        dataOutput.writeChars(serializedDocVector);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String serializedVector = dataInput.readLine();
        Gson gson = new Gson();
        DocVector docVector = gson.fromJson(serializedVector, DocVector.class);
        docId = docVector.docId;
        vector = docVector.vector;
    }

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public Map<Integer, Double> getVector() {
        return vector;
    }

    public void setVector(Map<Integer, Double> vector) {
        this.vector = vector;
    }

    @Override
    public int compareTo(DocVector o) {
        return 0;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
