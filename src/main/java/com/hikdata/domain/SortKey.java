package com.hikdata.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 组合键
 */
public class SortKey implements WritableComparable<SortKey> {

    private int first;
    private int second;

    /**
     * 默认构造方法必须有，否则会报<init>失败
     * java.lang.NoSuchMethodException: .<init>()
     */
    public SortKey() {

    }

    public SortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);

    }

    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();

    }


    @Override
    public int hashCode() {
        return first + "".hashCode() + second + "".hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SortKey) {
            SortKey k = (SortKey) obj;
            return k.first == first && k.second == second;
        }
        return false;
    }

    /**
     * 比较方法
     *
     * @param o
     * @return
     */
    public int compareTo(SortKey o) {
        if (first != o.first) {
            return o.first - first;
        } else if (second != o.second) {
            return o.second - second;
        }
        return 0;
    }
}
