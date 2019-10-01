package com.neu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class BusinessJoinMapper extends Mapper<Object, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outvalue = new Text();

    private Random rands = new Random();
    private Double percentage;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String strPercentage = context.getConfiguration().get("filter_percentage");
        percentage = Double.parseDouble(strPercentage);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String str[] = line.split(",");

        String businessId = str[0];

        if(!businessId.equalsIgnoreCase("#NAME?")){

            outKey.set(businessId);

            outvalue.set("A" + value.toString());

            if(rands.nextDouble() < percentage) {

//                System.out.println("AbusinessId = " + businessId);
//
//                System.out.println("Atext = " + outvalue.toString());

                context.write(outKey, outvalue);
            }
        }



    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }


}
