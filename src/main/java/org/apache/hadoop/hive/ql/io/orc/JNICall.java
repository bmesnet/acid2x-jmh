package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry.DeleteReaderValue;
import org.apache.hadoop.hive.ql.io.orc.ColumnizedDeleteEventRegistry.DeleteRecordKey;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import java.util.BitSet;
import java.util.Map.Entry;
import java.util.Iterator;

public class JNICall {


        //public native int PrintfakeDeleteDeltas(Iterator<Entry<DeleteRecordKey,DeleteReaderValue>> sortMerger);

        public native int SnapBuildHashTable(int index, DeleteRecordKey theKey);

        public native int SnapReadHashTable(VectorizedRowBatch batch, BitSet selectedBitSet);


        public JNICall(){
             System.loadLibrary("jnicall");

             System.out.println("library: "
                        + System.getProperty("java.library.path"));



        }

}
