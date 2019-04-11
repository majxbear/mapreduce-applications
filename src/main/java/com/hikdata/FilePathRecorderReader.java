package com.hikdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * 自定义RecordReader，强化LineRecordReader功能
 * 返回Key从LRR的行偏移量改变为每行文本所在的文档信息
 * Key:filename
 * Value:text
 */
public class FilePathRecorderReader extends RecordReader<Text, Text> {

    /**
     * 文件名
     */
    String filename = null;
    /**
     * 主要解析工作使用LineRecordReader实现
     */
    LineRecordReader lineRecordReader = new LineRecordReader();

    /**
     * 初始化方法
     *
     * @param split 数据分片
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //lineRecordReader初始化
        lineRecordReader.initialize(split, context);
        filename = ((FileSplit) split).getPath().getName();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        return lineRecordReader.nextKeyValue();
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(filename);
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentValue();
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {
        lineRecordReader.close();
    }
}
