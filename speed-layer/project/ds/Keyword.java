package org.example.ds;

public class Keyword implements Comparable<Keyword> {
    private String value;
    private int freq;

    public Keyword(String value) {
        this.value = value;
        this.freq = 1;
    }

    public Keyword(String value, int freq) {
        this.value = value;
        this.freq = freq;
    }

    public String getValue() {
        return value;
    }

    public int getFreq() {
        return freq;
    }

    public void increase(int amount) {
        freq += amount;
    }

    @Override
    public int compareTo(Keyword keyword) {
        if (this.value.equals(keyword.value)) {
            return 0;
        }
        return this.freq - keyword.freq;
    }

    @Override
    public String toString() {
        return "Keyword{" +
                "value='" + value + '\'' +
                ", freq=" + freq +
                '}';
    }
}
