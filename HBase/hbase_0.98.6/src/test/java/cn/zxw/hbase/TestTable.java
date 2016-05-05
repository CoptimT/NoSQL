package cn.zxw.hbase;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * 建立HBase表
 * 
 * @author hadoop
 *
 */
public class TestTable {

	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "10.10.25.13,10.10.25.14,10.10.25.15");
	}
    
	public static void main(String[] args) {
		String tableName = "user_info";
		String columnFamily = "basic";
		try {
			HTable table = new HTable(configuration, tableName);
			List<String> lines=FileUtils.readLines(new File(args[0]));
			List<Put> puts=new ArrayList<Put>();
			long ts=new Date().getTime();
			int n = 0;
			for(String line:lines){
				String[] arr = line.split("\t");
				String mac = arr[1];
				String phone = arr[2];
				String idfa = arr[3];
				String imei = arr[4];
				for(int i=0;i<8;i++){
					ts++;
					String rowKey = mac + ts;
					Put put = new Put(rowKey.getBytes());
					put.add(columnFamily.getBytes(), "mac".getBytes(), mac.getBytes());
					put.add(columnFamily.getBytes(), "phone".getBytes(), phone.getBytes());
					put.add(columnFamily.getBytes(), "idfa".getBytes(), idfa.getBytes());
					put.add(columnFamily.getBytes(), "imei".getBytes(), imei.getBytes());
					puts.add(put);
				}
				if(puts.size() > 10000){
					table.put(puts);
					puts.clear();
					System.out.println(++n + " 10000");
				}
			}
			if(puts.size() > 0){
				table.put(puts);
				puts.clear();
			}
			lines.clear();
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("finish");
	}
}
