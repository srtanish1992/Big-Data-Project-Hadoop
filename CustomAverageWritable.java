package com.neu;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomAverageWritable implements Writable {


    private Double sum = new Double(0);
    private long count = 1;

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeDouble(sum);
        dataOutput.writeLong(count);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        sum = dataInput.readDouble();
        count = dataInput.readLong();

    }

    public String toString() {
        return sum + "\t" + count;
    }
}
