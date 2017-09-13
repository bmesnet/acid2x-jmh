package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
   * An interface that can determine which rows have been deleted
   * from a given vectorized row batch. Implementations of this interface
   * will read the delete delta files and will create their own internal
   * data structures to maintain record ids of the records that got deleted.
   */
  public interface DeleteEventRegistry {
    /**
     * Modifies the passed bitset to indicate which of the rows in the batch
     * have been deleted. Assumes that the batch.size is equal to bitset size.
     * @param batch
     * @param selectedBitSet
     * @throws IOException
     */
    public void findDeletedRecords(VectorizedRowBatch batch, BitSet selectedBitSet) throws IOException;

    /**
     * The close() method can be called externally to signal the implementing classes
     * to free up resources.
     * @throws IOException
     */
    public void close() throws IOException;
  }