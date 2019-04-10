package com.hikdata.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 组合键
 */
public class RelationKey implements WritableComparable<RelationKey> {

    private String a;
    private String b;

    /**
     * 默认构造方法必须有，否则会报<init>失败
     * java.lang.NoSuchMethodException: .<init>()
     */
    public RelationKey() {

    }

    public RelationKey(String a, String b) {
        this.a = a;
        this.b = b;
    }

    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(a);
        dataOutput.writeUTF(b);

    }

    public void readFields(DataInput dataInput) throws IOException {
        a = dataInput.readUTF();
        b = dataInput.readUTF();
    }

    @Override
    public int hashCode() {
        return a.hashCode() + b.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RelationKey) {
            RelationKey k = (RelationKey) obj;
            return k.a == a && k.b == b;
        }
        return false;
    }

    public int compareTo(RelationKey o) {
        if (o.b.equals(a) && o.a.equals(b)) {
            return 0;
        }
        return 1;
    }
}
