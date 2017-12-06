package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import org.apache.hadoop.hive.ql.io.orc.JNICall;


public class ColumnizedDeleteEventRegistry implements DeleteEventRegistry {

  /**
   * A simple wrapper class to hold the (otid, bucketProperty, rowId) pair.
   */
  JNICall jni;
  public static class DeleteRecordKey implements Comparable<DeleteRecordKey> {
    private long originalTransactionId;
    /**
     * see {@link BucketCodec}
     */
    private int bucketProperty;
    private long rowId;

    public DeleteRecordKey() {
      this.originalTransactionId = -1;
      this.rowId = -1;
    }

    public void set(long otid, int bucketProperty, long rowId) {
      this.originalTransactionId = otid;
      this.bucketProperty = bucketProperty;
      this.rowId = rowId;
    }

    @Override
    public int compareTo(DeleteRecordKey other) {
      if (other == null) {
        return -1;
      }
      if (originalTransactionId != other.originalTransactionId) {
        return originalTransactionId < other.originalTransactionId ? -1 : 1;
      }
      if (bucketProperty != other.bucketProperty) {
        return bucketProperty < other.bucketProperty ? -1 : 1;
      }
      if (rowId != other.rowId) {
        return rowId < other.rowId ? -1 : 1;
      }
      return 0;
    }

    @Override
    public String toString() {
      return "otid: " + originalTransactionId + " bucketP:" + bucketProperty
          + " rowid: " + rowId;
    }
  } 

  public static class DeleteReaderValue {
    public DeleteReaderValue() {
    }
  }

  /**
   * A CompressedOtid class stores a compressed representation of the original
   * transaction ids (otids) read from the delete delta files. Since the record
   * ids are sorted by (otid, rowId) and otids are highly likely to be
   * repetitive, it is efficient to compress them as a CompressedOtid that
   * stores the fromIndex and the toIndex. These fromIndex and toIndex reference
   * the larger vector formed by concatenating the correspondingly ordered
   * rowIds.
   */
  private final class CompressedOtid implements Comparable<CompressedOtid> {
    final long originalTransactionId;
    final int bucketProperty;
    final int fromIndex; // inclusive
    final int toIndex; // exclusive

    CompressedOtid(long otid, int bucketProperty, int fromIndex, int toIndex) {
      this.originalTransactionId = otid;
      this.bucketProperty = bucketProperty;
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
    }

    @Override
    public int compareTo(CompressedOtid other) {
      // When comparing the CompressedOtid, the one with the lesser value is
      // smaller.
      if (originalTransactionId != other.originalTransactionId) {
        return originalTransactionId < other.originalTransactionId ? -1 : 1;
      }
      if (bucketProperty != other.bucketProperty) {
        return bucketProperty < other.bucketProperty ? -1 : 1;
      }
      return 0;
    }
  }

  private long rowIds[];
  private CompressedOtid compressedOtids[];
  private ValidTxnList validTxnList;


  public ColumnizedDeleteEventRegistry(ValidTxnList validTxns, Iterable<Entry<DeleteRecordKey,DeleteReaderValue>> sortMerger, int totalDeleteEventCount) {
    
    /* JNI INIT */

    JNICall myjni = new JNICall();
    this.jni = myjni;

    /* END JNI INIT */

    this.rowIds = null;
    this.validTxnList = validTxns;
    this.compressedOtids = null;
    this.rowIds = new long[totalDeleteEventCount];
    fakeDeleteDeltas(sortMerger);

  }

  private void fakeDeleteDeltas(
      Iterable<Entry<DeleteRecordKey, DeleteReaderValue>> sortMerger) {
    int distinctOtids = 0;
    long lastSeenOtid = -1;
    int lastSeenBucketProperty = -1;
    long otids[] = new long[rowIds.length];
    int[] bucketProperties = new int [rowIds.length];
    
    int index = 0;
    for (Entry<DeleteRecordKey, DeleteReaderValue> entry : sortMerger) {
      //Entry<DeleteRecordKey, DeleteReaderValue> entry = sortMerger.pollFirstEntry();
      DeleteRecordKey deleteRecordKey = entry.getKey();
      // DeleteReaderValue deleteReaderValue = entry.getValue();
      otids[index] = deleteRecordKey.originalTransactionId;
      bucketProperties[index] = deleteRecordKey.bucketProperty;
      rowIds[index] = deleteRecordKey.rowId;

      // Start of JNI insertion 
      jni.SnapBuildHashTable(index, deleteRecordKey);
      
      // End of JNI insertion 
      
      ++index;
      if (lastSeenOtid != deleteRecordKey.originalTransactionId ||
        lastSeenBucketProperty != deleteRecordKey.bucketProperty) {
        ++distinctOtids;
        lastSeenOtid = deleteRecordKey.originalTransactionId;
        lastSeenBucketProperty = deleteRecordKey.bucketProperty;
      }
      /*
      if (deleteReaderValue.next(deleteRecordKey)) {
        sortMerger.put(deleteRecordKey, deleteReaderValue);
      } else {
        deleteReaderValue.close(); // Exhausted reading all records, close the reader.
      } */
    }
    // Once we have processed all the delete events and seen all the distinct
    // otids,
    // we compress the otids into CompressedOtid data structure that records
    // the fromIndex(inclusive) and toIndex(exclusive) for each unique otid.
    this.compressedOtids = new CompressedOtid[distinctOtids];
    lastSeenOtid = otids[0];
    lastSeenBucketProperty = bucketProperties[0];
    int fromIndex = 0, pos = 0;
    for (int i = 1; i < otids.length; ++i) {
      if (otids[i] != lastSeenOtid
          || lastSeenBucketProperty != bucketProperties[i]) {
        compressedOtids[pos] = new CompressedOtid(lastSeenOtid,
            lastSeenBucketProperty, fromIndex, i);
        lastSeenOtid = otids[i];
        lastSeenBucketProperty = bucketProperties[i];
        fromIndex = i;
        ++pos;
      }
    }
    // account for the last distinct otid
    compressedOtids[pos] = new CompressedOtid(lastSeenOtid,
        lastSeenBucketProperty, fromIndex, otids.length);
  }

  private boolean isDeleted(long otid, int bucketProperty, long rowId) {
    if (compressedOtids == null || rowIds == null) {
      return false;
    }
    // To find if a given (otid, rowId) pair is deleted or not, we perform
    // two binary searches at most. The first binary search is on the
    // compressed otids. If a match is found, only then we do the next
    // binary search in the larger rowId vector between the given toIndex &
    // fromIndex.

    // Check if otid is outside the range of all otids present.
    if (otid < compressedOtids[0].originalTransactionId
        || otid > compressedOtids[compressedOtids.length - 1].originalTransactionId) {
      return false;
    }
    // Create a dummy key for searching the otid/bucket in the compressed otid
    // ranges.
    CompressedOtid key = new CompressedOtid(otid, bucketProperty, -1, -1);
    int pos = Arrays.binarySearch(compressedOtids, key);
    if (pos >= 0) {
      // Otid with the given value found! Searching now for rowId...
      key = compressedOtids[pos]; // Retrieve the actual CompressedOtid that
                                  // matched.
      // Check if rowId is outside the range of all rowIds present for this
      // otid.
      if (rowId < rowIds[key.fromIndex] || rowId > rowIds[key.toIndex - 1]) {
        return false;
      }
      if (Arrays.binarySearch(rowIds, key.fromIndex, key.toIndex, rowId) >= 0) {
        return true; // rowId also found!
      }
    }
    return false;
  }

  @Override
  public void findDeletedRecords(VectorizedRowBatch batch, BitSet selectedBitSet)
      throws IOException {

      // Start of JNI insertion 
      jni.SnapReadHashTable(batch, selectedBitSet);

      // End of JNI insertion 
      
    if (rowIds == null || compressedOtids == null) {
      return;
    }
    // in regular impl, this is called a function above
    findRecordsWithInvalidTransactionIds(batch, selectedBitSet);

    // Iterate through the batch and for each (otid, rowid) in the batch
    // check if it is deleted or not.

    long[] originalTransactionVector = batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION].isRepeating ? null
        : ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector;
    long repeatedOriginalTransaction = (originalTransactionVector != null) ? -1
        : ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_TRANSACTION]).vector[0];

    long[] bucketProperties = batch.cols[OrcRecordUpdater.BUCKET].isRepeating ? null
        : ((LongColumnVector) batch.cols[OrcRecordUpdater.BUCKET]).vector;
    int repeatedBucketProperty = (bucketProperties != null) ? -1
        : (int) ((LongColumnVector) batch.cols[OrcRecordUpdater.BUCKET]).vector[0];

    long[] rowIdVector = ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector;

    for (int setBitIndex = selectedBitSet.nextSetBit(0); setBitIndex >= 0; setBitIndex = selectedBitSet
        .nextSetBit(setBitIndex + 1)) {
      long otid = originalTransactionVector != null ? originalTransactionVector[setBitIndex]
          : repeatedOriginalTransaction;
      int bucketProperty = bucketProperties != null ? (int) bucketProperties[setBitIndex]
          : repeatedBucketProperty;
      long rowId = rowIdVector[setBitIndex];
      if (isDeleted(otid, bucketProperty, rowId)) {
        selectedBitSet.clear(setBitIndex);
      }
    }
  }
  
  private void findRecordsWithInvalidTransactionIds(VectorizedRowBatch batch,
      BitSet selectedBitSet) {
    if (batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION].isRepeating) {
      // When we have repeating values, we can unset the whole bitset at once
      // if the repeating value is not a valid transaction.
      long currentTransactionIdForBatch = ((LongColumnVector) batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION]).vector[0];
      if (!validTxnList.isTxnValid(currentTransactionIdForBatch)) {
        selectedBitSet.clear(0, batch.size);
      }
      return;
    }
    long[] currentTransactionVector = ((LongColumnVector) batch.cols[OrcRecordUpdater.CURRENT_TRANSACTION]).vector;
    // Loop through the bits that are set to true and mark those rows as false,
    // if their current transactions are not valid.
    for (int setBitIndex = selectedBitSet.nextSetBit(0); setBitIndex >= 0; setBitIndex = selectedBitSet
        .nextSetBit(setBitIndex + 1)) {
      if (!validTxnList.isTxnValid(currentTransactionVector[setBitIndex])) {
        selectedBitSet.clear(setBitIndex);
      }
    }
  }

  @Override
  public void close() throws IOException {
  }
}
