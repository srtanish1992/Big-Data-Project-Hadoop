package com.neu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class UserJoinMapper extends Mapper<Object, Text, Text, Text> {

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

        String test[] = line.split(",");

        if ((line.length() > 0 && !line.isEmpty()) && (test.length > 0)) {

            if(test[0].length() == 22 && !test[0].contains(" ")) {

//                System.out.println("line B = " + line);

                if (1 < test.length) {

                    if (!test[0].equalsIgnoreCase("#NAME?") && !test[1].equalsIgnoreCase("#NAME?")) {

                        StringBuilder input1 = new StringBuilder();

                        input1.append(line);

                        input1 = input1.reverse();

                        String str[] = input1.toString().split(",");

                        if (5 < str.length) {

                            String businessId = str[5];

                            StringBuilder bId = new StringBuilder();

                            bId.append(businessId);

                            bId = bId.reverse();

                            outKey.set(bId.toString());

                            outvalue.set("B" + value.toString());

                            if (rands.nextDouble() < percentage) {

//                        System.out.println("BbusinessId = " + bId.toString());
//
//                        System.out.println("Btext = " + outvalue.toString());



                                context.write(outKey, outvalue);
                            }


                        }

                    }
                }
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
