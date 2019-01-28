package com.zxw.hbase2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * It is a new API introduced in HBase 2.0 which aims to provide the ability to access HBase asynchronously.
 */
@SuppressWarnings("deprecation")
public class PrepareData {

	public static final String TABLE_NAME = "test";
	public static final String CF_DEFAULT = "t";
	private static Connection conn = null;
	private static Admin admin = null;
	private static Table table = null;

	public static void main(String... args) throws IOException {
		try {
			init();
			deleteTable(admin, TABLE_NAME);
			createTable(admin, TABLE_NAME, CF_DEFAULT);
			Map<String,String> values=new HashMap<String, String>();
			for(int i=1;i<=10;i++) {
				values.put("key"+i, "value"+i);
				writeRow(table, "row"+i, CF_DEFAULT, values);
			}
			scan(table, CF_DEFAULT);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			close();
		}
	}
	
	public static void init() throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "172.17.171.15");
		conn = ConnectionFactory.createConnection(config);
		admin = conn.getAdmin();
		table = conn.getTable(TableName.valueOf(TABLE_NAME));
		System.out.println("初始化完成!");
	}

	public static void deleteTable(Admin admin, String tableName) throws IOException {
		admin.disableTable(TableName.valueOf(tableName));
		admin.deleteTable(TableName.valueOf(tableName));
		System.out.println("删除表成功！");
	}
  
	public static void createTable(Admin admin, String tableName, String... cfs) throws IOException {
		if (admin.tableExists(TableName.valueOf(tableName))) {
			System.out.println("表已存在");
		} else {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
			for (String cf : cfs) {
				HColumnDescriptor family = new HColumnDescriptor(cf)
						.setCompressionType(Algorithm.NONE)//设置列簇压缩
						.setMaxVersions(HConstants.ALL_VERSIONS);//版本数
				table.addFamily(family);
			}
			admin.createTable(table);
			System.out.println("表创建成功");
		}
	}
	
	public static void writeRow(Table table, String rowKey,String cf,Map<String,String> values) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		for(String key:values.keySet()){
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(key), Bytes.toBytes(values.get(key)));
		}
		table.put(put);
		System.out.println("插入数据成功！");
	}
	
	public static void scan(Scan scan) throws IOException {
		ResultScanner rs = table.getScanner(scan);
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
				printResult(r);
			}
		} finally {
			rs.close();//always close the ResultScanner!
		}
	}
	
	public static void scan(Table table, String family) throws IOException {
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		//scan.setBatch(5);//查询结果每个Result最多包含列数，如果实际列数多于该值，则返回多个Result
		ResultScanner rs = table.getScanner(scan);
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
				printResult(r);
				System.out.println("----------");
			}
		} finally {
			rs.close();// always close the ResultScanner!
		}
	}
	
	public static void scan(Table table, String startRow, String stopRow, String family, String qualifier) throws IOException {
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		//scan.setRowPrefixFilter(Bytes.toBytes(ROW_KEY));
		scan.setStartRow(Bytes.toBytes(startRow)); // start key is inclusive
		scan.setStopRow(Bytes.toBytes(stopRow));   // stop key is exclusive
		ResultScanner rs = table.getScanner(scan);
		try {
		  for (Result r = rs.next(); r != null; r = rs.next()) {
			  //System.out.println(r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
			  printResult(r);
			  System.out.println("----------");
		  }
		} finally {
		  rs.close();//always close the ResultScanner!
		}
	}
	
	public static void printResult(Result r) {
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = r.getMap();// family,column,datas
		for (byte[] fbytes : map.keySet()) {
			System.out.println(new String(r.getRow()) + " -> " + new String(fbytes));
			NavigableMap<byte[], NavigableMap<Long, byte[]>> columnData = map.get(fbytes);// column,datas
			for (byte[] cbytes : columnData.keySet()) {
				NavigableMap<Long, byte[]> datas = columnData.get(cbytes);// datas
				String print = "          " + new String(cbytes) + " -> ";
				for (Long version : datas.keySet()) {
					print += "<" + version + "," + new String(datas.get(version)) + ">";
				}
				System.out.println(print);
			}
		}
	}
	
	public static void printResult2(Result r) {
		for (Cell cell : r.rawCells()) {
			System.out.print(new String(cell.getRowArray())+":"+new String(cell.getQualifierArray())+"="+new String(cell.getValueArray())+",");
		}
		System.out.println();
	}

	public static void close() throws IOException {
		if (table != null) {
			table.close();
		}
		if (admin != null) {
			admin.close();
		}
		if (conn != null) {
			conn.close();
		}
		System.out.println("关闭连接!");
	}

}
