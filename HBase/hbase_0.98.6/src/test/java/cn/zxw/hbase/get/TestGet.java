package cn.zxw.hbase.get;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TestGet {
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "10.10.25.13,10.10.25.14,10.10.25.15");
	}
    
	public static void main(String[] args) {
		String tableName = "user_applist";
		String columnFamily = "app";
		String qualifier = "applist";
		String rowKey="user1001";
		try {
			HTable table = new HTable(configuration, tableName);
			Get get=new Get(Bytes.toBytes(rowKey));
			Result result=table.get(get);
			System.out.println(result == null);
			byte[] b = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
			String res=new String(b);
			System.out.println(res);
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("finish");
	}
}
