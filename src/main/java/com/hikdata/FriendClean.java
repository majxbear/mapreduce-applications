package com.hikdata;

import com.hikdata.domain.RelationKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 去除好友关系中的重复关系
 * A C
 * C A
 * 只保留一份
 */
public class FriendClean {
    static class CleanMapper extends Mapper<Object, Text, RelationKey, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            context.write(new RelationKey(value.toString().split(" ")[0],
                    value.toString().split(" ")[1]), new Text(""));
        }
    }

    static class CleanReducer extends Reducer<RelationKey, Text, Text, Text> {
        @Override
        protected void reduce(RelationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.getA()), new Text(key.getB()));
            context.write(new Text(key.getB()), new Text(key.getA()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 2) {
            System.out.println("参数不足");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FriendClean.class);
        job.setMapperClass(CleanMapper.class);
        job.setCombinerClass(CleanReducer.class);
        job.setReducerClass(CleanReducer.class);

        job.setMapOutputKeyClass(RelationKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }
}
