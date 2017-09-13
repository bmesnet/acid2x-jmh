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
import org.openjdk.jmh.annotations.Scope;
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
    final FakeRowGenerator fake = new FakeRowGenerator(0, 999, 10_000, 0.01f);
    final Iterable<Entry<DeleteRecordKey, DeleteReaderValue>> deletes = fake
        .getDeletes();
    final ValidTxnList txns = fake.getValidTxns();
    final ColumnizedDeleteEventRegistry registry = new ColumnizedDeleteEventRegistry(
        txns, deletes, fake.getDeleteCount());
  }
  
  @Benchmark
  public void testGenerateRows(Acid2xState state) {
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
    for (VectorizedRowBatch batch = state.fake.next(null);
        batch != null;
        batch = state.fake.next(batch)) {
      bs.clear();
      state.registry.findDeletedRecords(batch, bs);
    }
  }
}
