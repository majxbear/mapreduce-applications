package com.hikdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogStatistics {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text val = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String str[] = value.toString().split(" ");
            //100010010 1001 C NEWS 1545271800010 103 200.41.1.25 W
            val.set(str[1] + " " + str[3]);
            context.write(new Text(str[2]), val);
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            System.out.println("参数不足");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置自定义的输出格式
        job.setOutputFormatClass(FileNameOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
