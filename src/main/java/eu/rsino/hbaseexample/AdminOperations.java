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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 *
 * @author rsinorchian
 */
public class AdminOperations {

    private final Configuration config;

    public AdminOperations() {
        this.config = HBaseManager.getConfig("192.168.48.156", "2181");
    }

    public void createTable(String tableName, List<String> columnFamilyList) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String columnFamilyName : columnFamilyList) {
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamilyName));
            }
            admin.createTable(tableDescriptor);
        } catch (ZooKeeperConnectionException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void deleteTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            admin.disableTable(tableName);
            // Deleting emp
            admin.deleteTable(tableName);
        } catch (ZooKeeperConnectionException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void deleteColumn(String tableName, String columnName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            admin.deleteColumn(tableName, columnName);
        } catch (ZooKeeperConnectionException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    public void enableTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);

            // Verifying weather the table is disabled
            Boolean bool = admin.isTableEnabled(tableName);

            // Disabling the table using HBaseAdmin object
            if (!bool) {
                admin.enableTable(tableName);
            }
        } catch (ZooKeeperConnectionException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void disableTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);

            // Verifying weather the table is disabled
            Boolean bool = admin.isTableEnabled(tableName);

            // Disabling the table using HBaseAdmin object
            if (!bool) {
                admin.disableTable(tableName);
            }
        } catch (ZooKeeperConnectionException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void addColumnToTable(String tableName, String columnFamilyName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilyName);
            // Adding column family
            admin.addColumn(tableName, columnDescriptor);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public boolean tableExists(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            return admin.tableExists(tableName);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public List<String> listAllTables() {
        List<String> tables = new ArrayList<>();
        try {
            HBaseAdmin admin = new HBaseAdmin(config);

            // Getting all the list of tables using HBaseAdmin object
            HTableDescriptor[] tableDescriptor = admin.listTables();

            // printing all the table names.
            for (int i = 0; i < tableDescriptor.length; i++) {
                tables.add(tableDescriptor[i].getNameAsString());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return tables;
    }
}
