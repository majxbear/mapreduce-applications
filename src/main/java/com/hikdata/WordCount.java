package com.hikdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * HADOOP自带WordCount示例
 * MR程序的基本机构包括一个Mapper的实现、一个Reducer的实现和main函数
 * map(映射操作)由Mapper类的map函数实现
 * reduce（规约操作）由Reducer类的reduce函数实现
 */
public class WordCount {
    /**
     * Mapper子类，继承Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     * 通过查看Mapper类的定义，四个泛型参数分别代表
     * <br>KEYIN<br/>：输入键值对的键（key）类型，在WordCount中代表每一行数据相对起始位置的偏移量，
     * 虽然标记为Object类型，但在实际运行过程中是LongWritable类型（相当于Long），
     * 在以单行文本为处理单元的MR程序中，此类型必须要能被MR框架处理，如果不能正确转换（如标记为Date类型），则会抛出错误
     * <p>
     * <br>VALUEIN<br/>：输入键值对的值（value）类型，map阶段输入的值，在WC程序中为一行行的文本
     * <p>
     * Map阶段的输出要根据业务灵活选择，不能局限
     * <br>KEYOUT<br/>：输出键值对的键（key）类型，在WC程序中为每一个分割出的单词
     * <br>VALUEOUT<br/>：输出键值对的键（value）类型，在WC程序中为每一个分割出的单词对应的计数
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * @param key     输入的键，文本相对起始位置的偏移量
         * @param value   输入的值，每行文本
         * @param context 全局对象，负责联通Map和Reduce，可简单理解为全局环境变量
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Mapper.Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * Reducer子类，继承Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     * 四个泛型参数分别代表
     * <br>KEYIN<br/>：输入键值对的键（key）类型，在WC中代表每个单词
     * <br>VALUEIN<br/>：输入键值对的值（value）类型，在WC程序中每个单词的计数
     * <p>
     * <br>KEYOUT<br/>：最终输出键值对的键（key）类型，在WC程序中为每一个分割出的单词
     * <br>VALUEOUT<br/>：最终输出键值对的键（value）类型，在WC程序中为每一个分割出的单词所有计数的和
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * @param key     每一个单词
         * @param values  Map阶段每一个单词的计数汇总，格式为(1,1,1,...)，
         *                其中的每个数字是由每一个Mapper统计出来的中间值，Reduce负责将这些值汇总统计，形成最后的输出
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        /**
         * 设置Combiner的作用是让每个Mapper汇总Map阶段的值，减少网络传输，提高效率
         */
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //输入路径
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //输出路径
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
