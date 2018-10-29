package com.index;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @Prigram: com.index
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-26 18:21
 */
public class IndexObserver extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        byte[] rowkey = put.getRow();

        List<Cell> cells = put.get(Bytes.toBytes("myfc1"), Bytes.toBytes("value"));
        for (Cell cell :
                cells) {
            byte[] value = cell.getValue();
            TableName tableName = e.getEnvironment().getRegion().getTableDesc().getTableName();
            HTableInterface table = e.getEnvironment().getTable(tableName);
            Put put1 = new Put(value).addColumn(Bytes.toBytes("myfc1"),Bytes.toBytes("i_vvv_rowkey"),rowkey);
            table.put(put1);
        }

    }
}
