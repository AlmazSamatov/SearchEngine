import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OutputDocument implements WritableComparable<OutputDocument> {

    private String title = "";
    private String url = "";
    private double relevance = 0;

    OutputDocument() {}

    OutputDocument(Document d){
        title = d.getTitle();
        url = d.getUrl() + String.valueOf(d.getId());
        relevance = d.getRelevance();
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public double getRelevance() {
        return relevance;
    }

    public void setRelevance(double relevance) {
        this.relevance = relevance;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(title);
        dataOutput.writeUTF(url);
        dataOutput.writeDouble(relevance);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        title = dataInput.readUTF();
        url = dataInput.readUTF();
        relevance = dataInput.readDouble();
    }

    @Override
    public int compareTo(OutputDocument o) {
        return Double.compare(o.relevance, relevance);
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

}
