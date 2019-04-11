package com.hikdata;

import com.hikdata.domain.WordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 使用自定义的FilePathInputFormat实现倒排索引
 */
public class RevertedIndex2 {

    /**
     * 输入的Key为文件名，输入的value为每行文本
     */
    static class WordMapper extends Mapper<Text, Text, WordWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("Key: " + key.toString() + ", value: " + value.toString());
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                WordWritable word = new WordWritable();
                word.setWord(tokenizer.nextToken());
                word.setFilePath(key.toString());
                context.write(word, new Text("1"));
            }
        }
    }

    static class WordCombiner extends Reducer<WordWritable, Text, WordWritable, Text> {
        @Override
        protected void reduce(WordWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text t : values) {
                sum += Integer.parseInt(t.toString());
            }
            context.write(key, new Text(key.getFilePath() + ":" + sum));
        }
    }

    static class WordReducer extends Reducer<WordWritable, Text, Text, Text> {
        @Override
        protected void reduce(WordWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text result = new Text();
            StringBuilder fileList = new StringBuilder();
            for (Text t : values) {
                fileList.append(t.toString()).append(";");
            }
            result.set(fileList.toString());
            context.write(new Text(key.getWord()), result);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 2) {
            System.out.println("At least 2 params must be given");
            System.exit(1);
        }
        String inputPath = args[0];
        String outPath = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("InvertedIndex");

        job.setJarByClass(RevertedIndex2.class);
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(WordWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(WordCombiner.class);
        job.setReducerClass(WordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置自定义的InputFormat
        job.setInputFormatClass(FilePathInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);

    }
}
