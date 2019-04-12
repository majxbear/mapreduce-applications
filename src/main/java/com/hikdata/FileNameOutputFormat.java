package com.hikdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * 自定义输出格式，实现以Key+".txt"为输出文件名称
 */
public class FileNameOutputFormat<K, V> extends FileOutputFormat<K, V> {
    public static String OUTPUT_FILE_EXTENSION = "txt";

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new MyRecordWriter(job, getTaskOutputPath(job));
    }

    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
        Path workPath = null;
        OutputCommitter committer = super.getOutputCommitter(conf);
        if (committer instanceof FileOutputCommitter) {
            workPath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            Path outputPath = super.getOutputPath(conf);
            if (outputPath == null) {
                throw new IOException("Undefined job output-path");
            }
            workPath = outputPath;
        }
        return workPath;
    }

    /**
     * 自定义实现RecordWriter，实现通过key指定文件名
     */
    public class MyRecordWriter extends RecordWriter<K, V> {
        private HashMap<String, RecordWriter<K, V>> recordWriters = null;
        private TaskAttemptContext job = null;
        private Path workPath = null;

        public MyRecordWriter(TaskAttemptContext job, Path workPath) {
            super();
            this.job = job;
            this.workPath = workPath;
            recordWriters = new HashMap<String, RecordWriter<K, V>>();
        }

        private String generateFileNameByKey(K key, String extension) {
            return key + "." + extension;
        }

        public void write(K key, V value) throws IOException, InterruptedException {
            //获取文件名的方法
            String baseName = generateFileNameByKey(key, OUTPUT_FILE_EXTENSION);
            RecordWriter<K, V> rw = this.recordWriters.get(baseName);
            if (rw == null) {
                rw = getBaseRecordWriter(job, baseName);
                this.recordWriters.put(baseName, rw);
            }
            rw.write(key, value);
        }

        private RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job, String baseName) throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            String keyValueSeparator = ":";
            RecordWriter<K, V> recordWriter = null;
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
                CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
                Path file = new Path(workPath, baseName + codec.getDefaultExtension());
                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);
                recordWriter = new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
            } else {
                Path file = new Path(workPath, baseName);
                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);
                recordWriter = new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
            }
            return recordWriter;
        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            Iterator<RecordWriter<K, V>> values = this.recordWriters.values().iterator();
            while (values.hasNext()) {
                values.next().close(context);
            }
            this.recordWriters.clear();
        }
    }

    /**
     * 代码与TextOutputFormat中的LineRecordWriter完全一致
     * 因为TextOutputFormat中的LineRecordWriter是protected，在不同包下无法引用
     * 所以需要重新定义
     */
    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;

        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text) o;
                out.write(to.getBytes(), 0, to.getLength());
            } else {
                out.write(o.toString().getBytes(utf8));
            }
        }

        public synchronized void write(K key, V value)
                throws IOException {
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                out.write(keyValueSeparator);
            }
            if (!nullValue) {
                writeObject(value);
            }
            out.write(newline);
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }
}
