import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.StringTokenizer;

public class WikiMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text docID = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        docID.set(fileSplit.getPath().getName());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        int index = 0;
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            context.write(docID, new Text(index + "," + word));
            index++;
        }
    }
}
