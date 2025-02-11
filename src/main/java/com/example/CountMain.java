package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountMain <input path> <output path>");
            System.exit(-1);
        }

        // Set up the Hadoop job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        job.setJarByClass(CountMain.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for the job to complete and exit based on its success
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
