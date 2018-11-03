import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.json.simple.JSONObject;
import org.json.simple.parser.*;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used in Search Engine application in order to extract required documents from data set using given IDs.
 */
public class ContentExtractor {



    /**
     * This method extracts information about docs from data set using provided IDs of pages provided as arguments.
     *
     * @param pages sorted list containing indexes of docs to extract.
     * @return response string
     * @throws Exception if something goes wrong
     */
    public static String extract(List<Integer> pages) throws Exception {

        List<String> outputList = new ArrayList<>(pages.size()); // list containing info about each document

        // Fill initial list with empty values:
        for (int i = 0; i < pages.size(); i++) {
            outputList.add("");
        }

        // Counter used to look for how many not found docs are left to find (needed to increase the performance of module):
        int pagesToFindLeft = pages.size();

        // Initialize required items:
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        String path = "hdfs:///EnWiki/AA_wiki_00";

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(path))))) {
            String line;
            line = bufferedReader.readLine();

            // Iterate through all the documents until end or all the documents needed are found:
            while (line != null && pagesToFindLeft > 0) {

                // Parse JSON:
                JSONObject jsonObject = (JSONObject) new JSONParser().parse(line);

                int id = Integer.parseInt((String) jsonObject.get("id"));
                String url = (String) jsonObject.get("url");
                String title = (String) jsonObject.get("title");

                int index = pages.indexOf(id);

                if (index != -1) {
                    // if document read is one we need to find:
                    outputList.set(index, title + "  " + url);
                    pagesToFindLeft--;
                }

                line = bufferedReader.readLine();
            }
        }

        StringBuilder response = new StringBuilder();

        // Convert list to string representing output response:
        for (String anOutputList : outputList) {
            response.append(anOutputList);
            response.append("\n");
        }

        return response.toString();
    }

    public static void main(String[] args) throws Exception{



    }
}
