package org.iansmith.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordSortMapper extends
        Mapper<Object, Text, IntWritable, Text> {

    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] pair = value.toString().split("\\s+");

        String stringWord = pair[0];
        word.set(stringWord);

        Integer count = new Integer(pair[1]);
        IntWritable countw = new IntWritable(count);

        context.write(countw, word);
    }
}