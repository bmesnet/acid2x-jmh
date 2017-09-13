package org.apache.hadoop.hive.ql.io.orc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SplittableRandom;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry.DeleteReaderValue;
import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry.DeleteRecordKey;

public class FakeDeleteGenerator {
  
  private long[] deleteBuckets;
  private BitSet[] deleteBits;
  private long[] buckets;
  int count = 0;
 
  
  public FakeDeleteGenerator(long[] buckets, long[] bucketCounts, float deleteFrac) {
    this.buckets = buckets;
    this.deleteBuckets = new long[buckets.length];
    this.deleteBits = new BitSet[buckets.length];
    SplittableRandom r = new SplittableRandom(buckets[0]);
    this.count = 0;
    for (int i = 0; i < bucketCounts.length; i++) {
      deleteBuckets[i] = (long) Math.ceil(bucketCounts[i] * deleteFrac);
      this.count += deleteBuckets[i];
      deleteBits[i] = new BitSet((int) bucketCounts[i]);
    }

    if (count == 0) {
      Arrays.fill(deleteBits, new BitSet()); // empty;
      return;
    }
    
    JDKRandomGenerator randomness = new JDKRandomGenerator();
    randomness.setSeed(count);
   
    for (int i = 0; i < buckets.length; i++) {
      ZipfDistribution zipf = new ZipfDistribution(randomness, 7, 0.5);
      for (int j = 0; j < deleteBuckets[i];) {
        int k = zipf.sample();
        int n = r.nextInt((int)bucketCounts[i]);
        if (k + j > deleteBuckets[i]) {
          k = (int) (deleteBuckets[i] - j);
        }
        // set k bits at random location
        for (int m = n; m < Math.min(n + k, deleteBits[i].size()); m++) {
          deleteBits[i].set(m);
        }
        j += k;
      }
    }
    
    // we round up deletes for the small case
    count = 0;
    for (int i = 0; i < buckets.length; i++) {
      final BitSet bs = deleteBits[i];
      count += bs.cardinality();
    }
  }

  private Entry<DeleteRecordKey, DeleteReaderValue> entry(final DeleteRecordKey key) {
    return new Entry<ColumnizedDeleteEventRegistry.DeleteRecordKey, ColumnizedDeleteEventRegistry.DeleteReaderValue>() {
      
      @Override
      public DeleteReaderValue setValue(DeleteReaderValue value) {
        // TODO Auto-generated method stub
        return null;
      }
      
      @Override
      public DeleteReaderValue getValue() {
        // TODO Auto-generated method stub
        return null;
      }
      
      @Override
      public DeleteRecordKey getKey() {
        return key;
      }
    };
  }

  private DeleteRecordKey key(int b, int row) {
    DeleteRecordKey dk = new DeleteRecordKey();
    dk.set(buckets[b], 0, row);
    return dk;
  }

  public Iterable<Entry<DeleteRecordKey, DeleteReaderValue>> getDeletes() {
    ArrayList<Entry<DeleteRecordKey, DeleteReaderValue>> deletes = new ArrayList<Entry<DeleteRecordKey, DeleteReaderValue>>(count);
    for (int i = 0; i < buckets.length; i++) {
      final BitSet bs = deleteBits[i];
      for (int j = bs.nextSetBit(0); j != -1; j = bs.nextSetBit(j + 1)) {
        deletes.add(entry(key(i,j)));
      }
    }
    return deletes;
  }
}
