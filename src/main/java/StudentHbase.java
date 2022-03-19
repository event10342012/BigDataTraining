
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

// docker run -d -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301 -p 16030:16030 -p 16020:16020 harisekhon/hbase

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
    }
}
