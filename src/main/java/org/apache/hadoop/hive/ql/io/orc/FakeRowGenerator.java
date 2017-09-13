package org.apache.hadoop.hive.ql.io.orc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SplittableRandom;
import java.util.TreeMap;

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry.DeleteReaderValue;
import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry.DeleteRecordKey;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class FakeRowGenerator {

  private int txncount = 11; // 11 txns, but non continous from start - end 
  private long start;
  private long end;
  private int rows;
  
  private long[] buckets = new long[txncount];
  private long[] bucketCounts = new long[txncount];
  // rows -> bucket mapping
  NavigableMap<Integer, Integer> rowMap = new TreeMap<Integer, Integer>();

  private int offset = 0;
  private FakeDeleteGenerator deleter;

  public FakeRowGenerator(long start, long end, int rawRowCounts,
      float deleteFrac) {
    this.start = start;
    this.end = end;
    this.rows = rawRowCounts;
    distributeRows();
    this.deleter = new FakeDeleteGenerator(buckets, bucketCounts, deleteFrac);
  }
  
  // distribute n-rows into buckets
  public void distributeRows() {
    SplittableRandom r = new SplittableRandom(start);
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = r.nextLong(start, end);
    }
    for (int i = 0; i < rows; i++) {
      int b = Math.abs(r.nextInt()) % txncount;
      bucketCounts[b]++;
    }
    // txns are in sorted order
    Arrays.sort(buckets);
    
    int rs = 0;
    for (int i = 0; i < buckets.length; i++) {
      rowMap.put(rs, i);
      rs += bucketCounts[i];
    }
  }
  
  public ValidTxnList getValidTxns() {
    // 19 exceptions (arbitrary)
    long minOpenTxn = start + (end - start)/2;
    long highWatermark = end-1;
    long[] exceptions = new long[19];
    int i = 0;
    for (long l = start; i < exceptions.length && l < end; l++) {
      if (l % 19 == 0) {
        exceptions[i] = l;
        i++;
      }
    }
    BitSet abortedBits = new BitSet();
    // arbitary minOpen
    return new ValidReadTxnList(exceptions, abortedBits, highWatermark, minOpenTxn);
  }
  
  public void reset() {
    this.offset = 0;
  }
  
  static VectorizedRowBatch createVrb() {
    List<String> names = new ArrayList<String>();
    List<TypeInfo> types = new ArrayList<TypeInfo>();
    TypeInfo longType = PrimitiveObjectInspectorFactory.writableLongObjectInspector.getTypeInfo();
    TypeInfo intType = PrimitiveObjectInspectorFactory.writableIntObjectInspector.getTypeInfo();
    names.add("operation"); types.add(intType);
    names.add("originalTransaction"); types.add(longType);
    names.add("bucket"); types.add(intType);
    names.add("rowId"); types.add(longType);
    names.add("currentTransaction"); types.add(longType);
    
    VectorizedRowBatch vrb = new VectorizedRowBatch(types.size());
    
    for (int i = 0; i < types.size(); i++) {
      TypeInfo typeInfo = types.get(i);
      vrb.cols[i] = VectorizedBatchUtil.createColumnVector(typeInfo);
    }
    vrb.reset();
    return vrb;
  }
  
  public VectorizedRowBatch next(VectorizedRowBatch batch) {
    if (offset >= rows) {
      return null;
    }
    // there are always enough rows to satisfy below this

    if (batch == null) {
      batch = createVrb();
    }

    batch.reset();

    int remaining = Math.min(rows - offset, VectorizedRowBatch.DEFAULT_SIZE);
    long minOtid = Long.MAX_VALUE;
    long maxOtid = Long.MIN_VALUE;

    for (int i = 0; i < remaining; i++) {
      // should not return NULL
      int zero = rowMap.floorKey(offset + i);
      int bucket = rowMap.get(zero);
      long rowId = offset + i - zero;
      long txnid = buckets[bucket];

      minOtid = Math.min(txnid, minOtid);
      maxOtid = Math.max(txnid, maxOtid);

      ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector[i] = txnid;
      ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector[i] = rowId;
    }

    if (minOtid == maxOtid) {
      VectorizedBatchUtil.setRepeatingColumn(batch,
          OrcRecordUpdater.ORIGINAL_TRANSACTION);
    }

    offset += remaining;
    VectorizedBatchUtil.setBatchSize(batch, remaining);

    Arrays.fill(
        ((LongColumnVector) batch.cols[OrcRecordUpdater.BUCKET]).vector, 0);
    VectorizedBatchUtil.setRepeatingColumn(batch, OrcRecordUpdater.BUCKET);

    VectorizedBatchUtil.setNoNullFields(batch);
    return batch;
  }
  
  public Iterable<Entry<DeleteRecordKey, DeleteReaderValue>> getDeletes() {
    return deleter.getDeletes();
  }

  public int getDeleteCount() {
    return deleter.count;
  }
}
