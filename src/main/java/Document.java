import org.apache.hadoop.io.WritableComparable;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Document implements WritableComparable<Document> {

    private int id;
    private String title = "";
    private String url = "";
    private String text = "";
    private double relevance = 0;

    Document() {
    }

    Document(String serialized) {
        try {
            JSONObject jsonObject = new JSONObject(serialized);
            if (jsonObject.has("id"))
                id = jsonObject.getInt("id");
            if (jsonObject.has("title"))
                title = jsonObject.getString("title");
            if (jsonObject.has("url"))
                title = jsonObject.getString("url");
            if (jsonObject.has("text"))
                title = jsonObject.getString("text");
            if (jsonObject.has("relevance"))
                relevance = jsonObject.getDouble("relevance");
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(title);
        dataOutput.writeUTF(url);
        dataOutput.writeUTF(text);
        dataOutput.writeDouble(relevance);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        title = dataInput.readUTF();
        url = dataInput.readUTF();
        text = dataInput.readUTF();
        relevance = dataInput.readDouble();
    }

    @Override
    public int compareTo(Document o) {
        return Double.compare(relevance, o.getRelevance());
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public double getRelevance() {
        return relevance;
    }

    public void setRelevance(double relevance) {
        this.relevance = relevance;
    }
}
