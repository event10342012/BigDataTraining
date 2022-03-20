import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HbaseOperator {
    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean createTable(String tableName, List<String> columnFamilies) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return false;
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String columnFamily : columnFamilies) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                tableDescriptor.addFamily(columnDescriptor);
            }
            admin.createTable(tableDescriptor);
        }
        return true;
    }

    public static boolean deleteTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        return true;
    }

    public static boolean putData(String tableName, String rowKey, String columnFamily, String qualifier, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
        table.close();
        return true;
    }

    public static void getData(String tableName, String rowKey, String columnFamily, String qualifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

    }

    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("Row: " + row + ", Family: " + family + ", Qualifier: " + qualifier + ", Value: " + value);
            }
        }

    }

    public static void deleteData(String tableName, String rowKey, String columnFamily, String qualifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        table.delete(delete);
    }

    public static void main(String[] args) throws IOException {
        List<String> columnFamilies = Arrays.asList("info", "score");
        String tableName = "student";

        HbaseOperator.createTable(tableName, columnFamilies);
        HbaseOperator.putData(tableName, "陳宏育", "info", "student_id", "G20220735020143");
        HbaseOperator.putData(tableName, "陳宏育", "info", "class", "1");
        HbaseOperator.putData(tableName, "陳宏育", "score", "understanding", "80");
        HbaseOperator.putData(tableName, "陳宏育", "score", "programming", "80");

        HbaseOperator.scanTable(tableName);
    }
}
