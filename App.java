package com.neu;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {

        Job job = Job.getInstance();
        job.setJarByClass(App.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,BusinessJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,UserJoinMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        job.setReducerClass(BusinessJoinReducer.class);

        job.getConfiguration().set("join-type","inner");

        job.getConfiguration().set("filter_percentage",".5");

        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path outDir = new Path(args[2]);
        if(fs.exists(outDir)){
            fs.delete(outDir,true);
        }

        job.setNumReduceTasks(1);

        boolean success = job.waitForCompletion(true);

        if (success) {

            Job job2 = Job.getInstance();

            job2.setJarByClass(App.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(CustomAverageWritable.class);

            job2.setMapperClass(AverageMapper.class);
            job2.setReducerClass(AverageReducer.class);

            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            FileSystem fs1 = FileSystem.get(job2.getConfiguration());
            Path outDirAvg = new Path(args[3]);
            if(fs1.exists(outDirAvg)){
                fs1.delete(outDirAvg,true);
            }

            job2.setNumReduceTasks(1);

            boolean success2 =job2.waitForCompletion(true);

            if (success2) {
                Job job3 = Job.getInstance();
                job3.setJarByClass(App.class);

                job3.setMapperClass(BusinessCountMapper.class);
                job3.setCombinerClass(BusinessCountReducer.class);
                job3.setReducerClass(BusinessCountReducer.class);

                job3.setInputFormatClass(TextInputFormat.class);
                job3.setOutputFormatClass(TextOutputFormat.class);

                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(IntWritable.class);

                FileInputFormat.addInputPath(job3, new Path(args[0]));
                FileOutputFormat.setOutputPath(job3, new Path(args[4]));

                FileSystem fs2 = FileSystem.get(job3.getConfiguration());
                Path outDir2 = new Path(args[4]);
                if(fs2.exists(outDir2)){
                    fs2.delete(outDir2,true);
                }

                job3.setNumReduceTasks(1);

                job3.waitForCompletion(true);
            }
        }
    }
}
