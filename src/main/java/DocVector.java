import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class DocVector implements Writable {
    private int docId;
    private Map<Integer, Double> vector;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(docId);
        Gson gson = new Gson();
        String serializedVector = gson.toJson(vector);
        dataOutput.writeChars(serializedVector);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        docId = dataInput.readInt();
        String serializedVector = dataInput.readLine();
        Gson gson = new Gson();
        vector = gson.fromJson(serializedVector, new TypeToken<Map<Integer, Double>>() {}.getType());
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
}
