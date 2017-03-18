package org.iansmith.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    private String separator = ",";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String sep = context.getConfiguration().get("separator");

        if (sep != null)
            this.separator = sep;

    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] csv = value.toString().split(separator);
        for (String str : csv) {
            word.set(str);
            context.write(word, ONE);
        }
    }
}