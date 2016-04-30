/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.rsino.hbaseexample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;

/**
 *
 * @author rsinorchian
 */
public class ClientOperations {

    private final Configuration config;

    public ClientOperations() {
        this.config = HBaseManager.getConfig("192.168.48.156", "2181");
    }

    public Boolean deleteCell(String tableName, String rowName, String columnFamilyName, String columnName) {
        try (HTable hTable = new HTable(config, tableName)) {

            Delete delete = new Delete(Bytes.toBytes(rowName));
            delete.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
            //delete.deleteFamily(Bytes.toBytes("professional"));
            hTable.delete(delete);
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public Boolean deleteFamily(String tableName, String rowName, String columnFamilyName) {
        try (HTable hTable = new HTable(config, tableName)) {

            Delete delete = new Delete(Bytes.toBytes(rowName));
            delete.addFamily(Bytes.toBytes(columnFamilyName));
            hTable.delete(delete);
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public Boolean deleteRow(String tableName, String rowName) {
        try (HTable hTable = new HTable(config, tableName)) {

            Delete delete = new Delete(Bytes.toBytes(rowName));
            hTable.delete(delete);
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public String getCellData(String tableName, String rowName, String columnFamilyName, String columnName) {
        String cellData = "";
        try (HTable hTable = new HTable(config, tableName)) {

            // Instantiating Get class
            Get g = new Get(Bytes.toBytes(rowName));

            // Reading the data
            Result result = hTable.get(g);

            byte[] value = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
            cellData = Bytes.toString(value);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return cellData;
    }

    public List<String> scanTable(String tableName, List<String> columns) {
        List<String> results = new ArrayList<>();
        try {
            // Instantiating HTable class
            HTable table = new HTable(config, tableName);
            // Instantiating the Scan class
            Scan scan = new Scan();
            int rowCount = columns.size();
            for (int i = 0; i < rowCount; i++) {
                String columnFamilyName = columns.get(i).split(":")[0];
                String columnName = columns.get(i).split(":")[1];
                scan.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
            }
            // Getting the scan result
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    results.add(result.toString());
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return results;
    }

    public Boolean updateCell(String tableName, String rowName, String columnFamilyName, String columnName, String value) {
        try (HTable hTable = new HTable(config, tableName)) {
            Put p = new Put(Bytes.toBytes(rowName));
            p.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
            hTable.put(p);
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public void insertRow(String tableName, String rowName, String columnFamilyName, String columnName, String value) {
        try (HTable hTable = new HTable(config, tableName)) {

            Put p = new Put(Bytes.toBytes(rowName));

            // adding values using add() method
            // accepts column family name, qualifier/row name ,value
            p.addColumn(Bytes.toBytes(columnFamilyName),
                    Bytes.toBytes(columnName), Bytes.toBytes(value));

            hTable.put(p);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
