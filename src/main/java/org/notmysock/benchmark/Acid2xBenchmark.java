package org.notmysock.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry.DeleteReaderValue;
import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry.DeleteRecordKey;
import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry;
import org.apache.hadoop.hive.ql.io.orc.FakeRowGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations=2)
@Measurement(batchSize = 10, iterations=3)
public class Acid2xBenchmark {
  @State(Scope.Benchmark)
  public static class Acid2xState {
    
    @Param({"10000", "1000000"})
    public int rows;
    
    @Param({"0.01f", "0.1f"})
    public float deleteFrac;
    
    public FakeRowGenerator fake;
    public Iterable<Entry<DeleteRecordKey, DeleteReaderValue>> deletes;
    public ValidTxnList txns;
    public ColumnizedDeleteEventRegistry registry;
  }
  
  
  @Setup
  public void setUp(Acid2xState state) {
    System.out.println("Setting up for " + state.rows);
    state.fake = new FakeRowGenerator(0, 999, state.rows, state.deleteFrac);
    state.deletes = state.fake
        .getDeletes();
    state.txns = state.fake.getValidTxns();
    state.registry = new ColumnizedDeleteEventRegistry(
        state.txns, state.deletes, state.fake.getDeleteCount());
  }
  
  
  
  @Benchmark
  public void testGenerateRows(Acid2xState state) {
    state.fake.reset();
    for (VectorizedRowBatch batch = state.fake.next(null);
        batch != null;
        batch = state.fake.next(batch)) {
    }
  }
  
  @Benchmark
  public void testLoadDeletes(Acid2xState state) throws IOException {
    ColumnizedDeleteEventRegistry registry = new ColumnizedDeleteEventRegistry(
        state.txns, state.deletes, state.fake.getDeleteCount());
  }
  
  @Benchmark
  public void testFilterRows(Acid2xState state) throws IOException {
    BitSet bs = new BitSet(VectorizedRowBatch.DEFAULT_SIZE);
    state.fake.reset();
    for (VectorizedRowBatch batch = state.fake.next(null);
        batch != null;
        batch = state.fake.next(batch)) {
      bs.clear();
      state.registry.findDeletedRecords(batch, bs);
    }
  }
}
