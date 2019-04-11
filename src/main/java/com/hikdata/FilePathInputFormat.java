package com.hikdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义InputFormat，提取每行文本所在的文档信息
 * Key:filename
 * Value:text
 */
public class FilePathInputFormat extends FileInputFormat<Text, Text> {
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FilePathRecorderReader pathRecorderReader = new FilePathRecorderReader();
        pathRecorderReader.initialize(split, context);
        return pathRecorderReader;
    }
}
