package com.neu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageReducer extends Reducer<Text, CustomAverageWritable, Text, CustomAverageWritable> {

    private CustomAverageWritable result = new CustomAverageWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }


    @Override
    public void reduce(Text key, Iterable<CustomAverageWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0;
        long count = 0;

        for (CustomAverageWritable customAverageTuple : values) {
            sum = sum + customAverageTuple.getSum() * customAverageTuple.getCount();
            count = count + customAverageTuple.getCount();

        }
        result.setCount(count);
        result.setSum(sum / count);

        context.write(new Text(key.toString()), result);
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
