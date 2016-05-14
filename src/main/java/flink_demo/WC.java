package flink_demo;

/**
 * Created by philipp on 14.05.16.
 */
public class WC {

    public WC(String word, int count) {
        this.word = word; this.c = count;
    }

    public WC() {} // empty constructor to satisfy POJO requirements

    public String word;
    public int c;

    @Override
    public String toString() {
        return word + ":" + c;
    }
}
