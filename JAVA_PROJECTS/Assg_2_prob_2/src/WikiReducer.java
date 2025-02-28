import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class WikiReducer extends Reducer<Text, Text, IntWritable, Text> {
    private IntWritable index = new IntWritable();

    @Override
    public void reduce(Text docID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> wordsList = new ArrayList<>();
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length == 2) {
                index.set(Integer.parseInt(parts[0]));
                wordsList.add(parts[1]);
                context.write(index, new Text("(" + docID.toString() + ", " + parts[1] + ")"));
            }
        }
    }
}
