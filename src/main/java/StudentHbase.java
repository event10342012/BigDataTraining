
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class StudentHbase {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:16000");
        Connection conn = ConnectionFactory.createConnection(configuration);

        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("user_t");
        String colFamily = "User";
        int rowKey = 1;

        // create table
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exist");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create success");
        }

        // insert data
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("uid"), Bytes.toBytes("001"));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("name"), Bytes.toBytes("Tom"));
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success");

        // select data
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()){
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell: result.rawCells()){
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // delete data
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete success");

        // delete table
        if (admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table delete success");
        } else{
            System.out.println("Table does not exists");
        }

    }
}
