package com.hikdata;

import com.hikdata.domain.SortKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 数据二次排序
 * 数据格式如
 * 98 87
 * 98 85
 * 99 89
 */
public class DataSecSort {
    /**
     * map将输入中的value化成组合键类型，作为输出的key
     */
    public static class SortMapper extends Mapper<Object, Text, SortKey, IntWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            /**
             * 写出的键值对格式 ((99,89),89)
             */
            context.write(new SortKey(Integer.parseInt(line.split(" ")[0]),
                            Integer.parseInt(line.split(" ")[1])),
                    new IntWritable(Integer.parseInt(line.split(" ")[1]))
            );
        }
    }


    public static class SortReducer extends Reducer<SortKey, IntWritable, Text, IntWritable> {
        public void reduce(SortKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(new Text(String.valueOf(key.getFirst())), val);
            }
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
        job.setJobName("word count");
        job.setJarByClass(DataSecSort.class);
        job.setMapperClass(DataSecSort.SortMapper.class);
        job.setReducerClass(DataSecSort.SortReducer.class);
        /**
         * 设置map输出key-value类型
         */
        job.setMapOutputKeyClass(SortKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        /**
         * 设置reduce输出key-value类型
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setGroupingComparatorClass(GroupingComparator.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    static class GroupingComparator implements RawComparator<SortKey> {
        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            return WritableComparator.compareBytes(bytes, i, Integer.SIZE / 8, bytes1, i2, Integer.SIZE / 8);
        }

        public int compare(SortKey o1, SortKey o2) {
            return o1.getFirst() - o2.getFirst();
        }
    }
}
