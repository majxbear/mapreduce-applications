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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 数据格式如下
 * A B
 * B C
 * A D
 * D C
 * 输出A-C 2:B,D
 * 业务场景：二度好友发现，A和B的共同好友有几个，都是谁
 */
public class CommonFriends {
    static class RelationMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s");
            //原样输出
            context.write(new Text(words[0].trim()), new Text(words[1].trim()));
        }
    }

    static class RelationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> friends = new ArrayList<Text>();
            for (Text val : values) {
                friends.add(new Text(val.toString()));
            }
            StringBuilder builder = new StringBuilder();
            for (Text f : friends) {
                builder.append(f.toString()).append(",");
            }
            //去除末尾的,
            builder = new StringBuilder(builder.substring(0, builder.length() - 1));
            context.write(new Text(key.toString() + ":" + builder.toString()), null);
        }
    }

    /**
     * 输入：A：B,C,D
     * 输出:((B-C),A)((B-D),A)((C-D),A)
     */
    static class FriendMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(":");
            String user = words[0];
            String[] friends = words[1].split(",");
            //排序：避免出现A-B、B-A重复输出的情况
            Arrays.sort(friends);
            //循环两两匹配
            for (int i = 0; i < friends.length - 1; i++) {
                for (int j = i + 1; j < friends.length; j++) {
                    context.write(new Text(friends[i] + "-" + friends[j]), new Text(user));
                }
            }
        }
    }

    static class FriendReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            StringBuilder builder = new StringBuilder();
            for (Text val : values) {
                count += 1;
                builder.append(val.toString()).append(" ");
            }
            //去除末尾的空格
            builder = new StringBuilder(builder.substring(0, builder.length() - 1));
            String output = key + " " + count + ":" + builder.toString();
            context.write(new Text(output), null);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < AppConstants.PARAMS_MIN_2) {
            System.out.println("参数不足");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        String tempPath = "tmp";
        Configuration conf = new Configuration();

        //job1
        Job job = Job.getInstance(conf);
        job.setJobName("relation analysis");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(CommonFriends.RelationMapper.class);
        job.setReducerClass(CommonFriends.RelationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(tempPath));

        job.waitForCompletion(true);

        //job2 聚合
        Job job2 = Job.getInstance(conf);
        job2.setJobName("relation analysis");
        job2.setJarByClass(CommonFriends.class);
        job2.setMapperClass(FriendMapper.class);
        job2.setReducerClass(FriendReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);

        //在实际生产环境中，如果文件较多，会使用HDFS的合并api将其合并，存放在hdfs的指定目录下供本阶段使用
        FileInputFormat.setInputPaths(job2, new Path(tempPath + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));
        job2.waitForCompletion(true);
    }
}
