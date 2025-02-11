package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into words using space as the delimiter
        String[] words = value.toString().split("\\s+");

        for (String w : words) {
            if (!w.isEmpty()) {
                word.set(w.replaceAll("[^a-zA-Z0-9]", "").toLowerCase()); // Normalize by removing punctuation and lowercasing
                context.write(word, one);
            }
        }
    }
}
