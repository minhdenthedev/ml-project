package org.example.ds;

import java.util.Arrays;
import java.util.Comparator;

public class Heap {
    private class Entry {
        int id;
        int value;
        int num_occurance;
        Entry(int id) {
            this.id = id;
            this.value = 0;
            this.num_occurance = 0;
        }

        public int getId() {
            return id;
        }

        public int getValue() {
            return value;
        }

        public void increase(int amount) {
            value += amount;
        }

        public String toString() {
            StringBuilder res = new StringBuilder();
            res.append(Integer.toString(id)).append("|").append(Integer.toString(value)).append("|").append(num_occurance).append("|").append(value * 1.0 / num_occurance);
            return res.toString();
        }
    }

    private int[] entriesIndex;
    private Entry[] entries;
    private final Comparator<Entry> comparator;
    private static final int DEFAULT_CAPACITY = 2000;
    private int n;

    public int size() {
        return n;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Heap(CompareStrategy compareStrategy, int initialSize) {
        switch (compareStrategy) {
            case TOTAL:
                this.comparator = new TotalComparator();
                break;
            default:
                this.comparator = new AverageComparator();
        }
        this.entriesIndex = new int[initialSize];
        this.entries = new Entry[initialSize];
        this.n = 0;
    }

    public Heap(CompareStrategy compareStrategy) {
        this(compareStrategy, DEFAULT_CAPACITY);
    }

    public void allocateMore() {
        this.entries = Arrays.copyOf(this.entries, this.entries.length * 2);
        this.entriesIndex = Arrays.copyOf(this.entriesIndex, this.entriesIndex.length * 2);
    }

    public void update(int id, int value) {
        int index = entriesIndex[id];

        if (index <= 0) {
            if (n >= entries.length) {
                allocateMore();
            }
            Entry newEntry = new Entry(id);
            newEntry.increase(value);
            newEntry.num_occurance++;
            entries[++n] = newEntry;
            entriesIndex[id] = n;
            upheap(n, id);
        } else {
            entries[index].increase(value);
            entries[index].num_occurance++;
            upheap(index, id);
            downheap(entriesIndex[id], id);
        }
    }

    public Entry poll() {
        if (isEmpty()) {
            return null;
        }
        swap(entries, 1, n, entriesIndex, entries[1].getId(), entries[n].getId());
        n--;
        Entry res = entries[n + 1];
        downheap(1, entries[1].getId());
        entries[n + 1] = null;
        entriesIndex[res.getId()] = 0;
        return res;
    }

    private void upheap(int pos, int id) {
        while (pos > 1 && compare(entries[pos], entries[pos / 2]) > 0) {
            swap(entries, pos, pos / 2, entriesIndex, entries[pos].getId(), entries[pos / 2].getId());
            pos /= 2;
        }
    }

    private void downheap(int pos, int id) {
        while (pos * 2 <= n) {
            int i = pos * 2;
            if (i < n && compare(entries[i], entries[i + 1]) < 0) {
                i++;
            }
            if (compare(entries[pos], entries[i]) > 0) {
                break;
            }
            swap(entries, pos, i, entriesIndex, entries[pos].getId(), entries[i].getId());
            pos = i;
        }
    }

    private int compare(Entry entry1, Entry entry2) {
        return comparator.compare(entry1, entry2);
    }

    private void swap(Entry[] entries, int i, int j, int[] entriesIndex, int id1, int id2) {
        Entry tempIndex = entries[i];
        entries[i] = entries[j];
        entries[j] = tempIndex;

        int tempId = entriesIndex[id1];
        entriesIndex[id1] = entriesIndex[id2];
        entriesIndex[id2] = tempId;
    }

    private class TotalComparator implements Comparator<Entry> {
        @Override
        public int compare(Entry entry1, Entry entry2) {
            return entry1.value - entry2.value;
        }
    }

    private class AverageComparator implements Comparator<Entry> {
        @Override
        public int compare(Entry entry1, Entry entry2) {
            return Double.compare(
                    (entry1.value * 1.0) / entry1.num_occurance, (entry2.value * 1.0) / entry2.num_occurance
            );
        }
    }
}
