package cn.zxw.hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import cn.zxw.hbase.constant.Constants;

public class HBaseConnectUtil {
	private static Configuration conf=HBaseConfiguration.create();
	private static HBaseAdmin admin=null;
	private static HTable table=null;
	
	static{
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "10.10.25.13,10.10.25.14,10.10.25.15");
		System.out.println("Configuration setting ...");
	}
	
	private HBaseConnectUtil() {
		super();
	}

	public static Configuration getConf() {
		return conf;
	}
	
	public static HBaseAdmin getAdmin() throws IOException {
		if(admin==null){
			admin=new HBaseAdmin(conf);
		}
		return admin;
	}

	public static HTable getTable(String tableName) throws IOException {
		if(table==null){
			table = new HTable(conf, Constants.TB_NAME_OBJ);
		}
		return table;
	}
	
	public static void close() throws IOException {
		System.out.println("Close runing ...");
		if(table!=null){
			table.close();
		}
		if(admin!=null){
			admin.close();
		}
	}
	
}
