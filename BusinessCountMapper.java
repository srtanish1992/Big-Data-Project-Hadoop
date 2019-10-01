package com.neu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BusinessCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);


    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();

        StringBuilder token = new StringBuilder();

        token.append(line);

        token = token.reverse();

        String[] tokens = token.toString().split(",");

        StringBuilder firstToken = new StringBuilder();

        firstToken.append(tokens[0]);

        firstToken = firstToken.reverse();

        context.write(new Text(firstToken.toString()),one);


    }

    @Override
    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Mapper.Context context) throws IOException, InterruptedException {
        super.run(context);
    }


}
