package com.neu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageMapper extends Mapper<Object, Text, Text, CustomAverageWritable> {

    private CustomAverageWritable sumCount = new CustomAverageWritable();
    private Text businessId = new Text();



    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();

        StringBuilder input1 = new StringBuilder();

        input1.append(line);

        input1 = input1.reverse();

//        System.out.println(input1);

        String str[] = input1.toString().split(",");

//        for(int i = 0; i < str.length;i++){
//            System.out.println(str[i]);
//        }

//        System.out.println(str[5]);

//        System.out.println(str[1]);

        if(str[0].matches("^\\d+$")) {

            StringBuilder bId = new StringBuilder();

            bId.append(str[5]);

            bId = bId.reverse();

            businessId.set(bId.toString());

            sumCount.setCount(1);
            sumCount.setSum(Double.parseDouble(str[3]));

            context.write(businessId,sumCount);

        }




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
