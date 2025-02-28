import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class LatestWordReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private Text latestWord = new Text();

    @Override
    public void reduce(IntWritable index, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int maxDocID = Integer.MIN_VALUE;
        String wordWithMaxDocID = "";

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length < 2) continue;

            try {
                int docID = Integer.parseInt(parts[0]); 
                String word = parts[1];

                if (docID > maxDocID) {
                    maxDocID = docID;
                    wordWithMaxDocID = word;
                }
            } catch (NumberFormatException e) {
                continue;
            }
        }

        latestWord.set(wordWithMaxDocID);
        context.write(index, latestWord); 
    }
}
