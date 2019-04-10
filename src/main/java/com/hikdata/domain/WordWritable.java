package com.hikdata.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordWritable implements WritableComparable<WordWritable> {
    /**
     * 词项
     */
    private String word;
    /**
     * 词项出现的文档路径
     */
    private String filePath;

    /**
     * 默认构造方法
     */
    public WordWritable() {
    }

    /**
     * 带属性的构造方法
     *
     * @param word
     * @param filePath
     */
    public WordWritable(String word, String filePath) {
        this.word = word;
        this.filePath = filePath;
    }

    /**
     * getter setter
     *
     * @return
     */
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    /**
     * 排序方法，让MR自动排序
     * 使用String的compareTo方法按字母顺序排列
     *
     * @param o
     * @return
     */
    public int compareTo(WordWritable o) {
        if (word != o.word) {
            return word.compareTo(o.word);
        }
        return filePath.compareTo(o.filePath);
    }

    /**
     * 序列化方法
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeUTF(filePath);
    }

    /**
     * 反序列化方法
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        filePath = in.readUTF();
    }
}
