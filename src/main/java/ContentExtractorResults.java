import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ContentExtractorResults implements WritableComparable<ContentExtractorResults> {

    private Document document = new Document();
    private double relevance;

    ContentExtractorResults() {}

    ContentExtractorResults(Document document, double relevance) {
        this.document = document;
        this.relevance = relevance;
    }

    @Override
    public int compareTo(ContentExtractorResults o) {
        return Double.compare(relevance, o.getRelevance());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        document.write(dataOutput);
        dataOutput.writeDouble(relevance);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        document.readFields(dataInput);
        relevance = dataInput.readDouble();
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public double getRelevance() {
        return relevance;
    }

    public void setRelevance(double relevance) {
        this.relevance = relevance;
    }
}
