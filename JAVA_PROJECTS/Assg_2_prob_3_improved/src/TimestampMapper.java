import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class TimestampMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable indexKey = new IntWritable();
    private Text timestampWordPair = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        System.out.println("Processing line: " + line); 


        if (!line.contains("(") || !line.contains(")")) {
            System.err.println("Skipping malformed line: " + line);
            return;
        }

        try {
            int openParen = line.indexOf("(");
            int closeParen = line.lastIndexOf(")");
            int commaIndex = line.indexOf(",");
            
            int index = Integer.parseInt(line.substring(0, openParen).trim()); 
            String[] docWord = line.substring(openParen + 1, closeParen).split(",");
            
            if (docWord.length < 2) {
                System.err.println("Skipping malformed entry: " + line);
                return;
            }

            String docIDStr = line.substring(openParen + 1, commaIndex).trim().replace(".txt", "");
            int docID = Integer.parseInt(docIDStr); 
            String word = line.substring(commaIndex + 1, closeParen).trim();
            
            
            


            indexKey.set(index);
            timestampWordPair.set(docID + "," + word);
            
            context.write(indexKey, timestampWordPair);
        } catch (Exception e) {
            System.err.println("Error processing line: " + line + " | Exception: " + e.getMessage());
        }
    }
    
}

