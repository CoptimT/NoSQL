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
		String tableName = "dmp_asdk";
		String columnFamily = "app";
		String qualifier = "appboot";
		String cf_info = "info";
		String col_tags = "tags";
		String rowKey="000099efa8553f9385f90b4cc30a8c64";
		try {
			HTable table = new HTable(configuration, tableName);
			Get get=new Get(Bytes.toBytes(rowKey));
			//get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
			Result result=table.get(get);
			byte[] appboot = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
			if(appboot != null){
				System.out.println(new String(appboot));
			}
			byte[] tags = result.getValue(Bytes.toBytes(cf_info), Bytes.toBytes(col_tags));
			if(tags != null){
				System.out.println(new String(tags));
			}
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("finish");
	}
}
