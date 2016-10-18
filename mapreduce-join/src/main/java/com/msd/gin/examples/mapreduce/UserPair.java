package com.msd.gin.examples.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserPair implements WritableComparable<UserPair> {

    private Text first;
    private Text second;

    public UserPair() {
        set(new Text(), new Text());
    }

    public UserPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public UserPair(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof UserPair) {
            UserPair tp = (UserPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public int compareTo(UserPair pair) {
        int compare = first.compareTo(pair.first);
        if (compare != 0) {
            return compare;
        }
        return second.compareTo(pair.second);
    }
}