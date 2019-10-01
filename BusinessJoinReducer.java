package com.neu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;

import java.io.IOException;

public class BusinessJoinReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_TEXT = new Text("");
    private Text tmp = new Text();
    private ArrayList<Text> listA = new ArrayList<>();
    private ArrayList<Text> listB = new ArrayList<>();
    private String joinType = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        joinType = context.getConfiguration().get("join-type");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        listA.clear();
        listB.clear();

        for(Text value: values){
            tmp = value;
            if(tmp.charAt(0) == 'A'){
                listA.add(new Text(tmp.toString().substring(1).concat(",")));
            } else if (tmp.charAt(0)=='B'){
                listB.add(new Text(tmp.toString().substring(1)));
            }
        }

        executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {

        if(joinType.equalsIgnoreCase("inner")){
            if(!listA.isEmpty() && !listB.isEmpty()){
                for(Text A : listA) {
                    for(Text B : listB){

//                        System.out.println("Text A = " + A.toString());
//
//                        System.out.println("Text B = " + B.toString());

                        context.write(A,B);
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
