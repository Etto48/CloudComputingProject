package it.unipi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Char implements WritableComparable<Char> {
    private char c;

    public Char() {
        c = 0;
    }

    public Char(char c) {
        this.c = c;
    }

    public void set(char c) {
        this.c = c;
    }

    public char get() {
        return c;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        c = arg0.readChar();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeChar(c);
    }

    @Override
    public int compareTo(Char o) {
        return Character.compare(c, o.c);
    }

    @Override
    public int hashCode() {
        return Character.hashCode(c);
    }

    @Override
    public String toString() {
        return Character.toString(c);
    }
}
