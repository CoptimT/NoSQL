package cn.zxw.hbase.get;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TestEx1 {
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "10.10.25.204,10.10.25.205,10.10.25.206");
	}
    
	public static void main(String[] args) {
		String tableName = "dmp_reyun_tagid";
		String columnFamily = "info";
		String qualifier = "tags";
		try {
			HTable table = new HTable(configuration, tableName);
			List<String> imeis=FileUtils.readLines(new File("D:\\wanka\\data\\imei_sdk.txt"));
			for(String imei:imeis){
				imei=StringUtils.reverse(imei);
				Get get=new Get(Bytes.toBytes(imei));
				Result result=table.get(get);
				byte[] b = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
				if(b != null){
					System.out.println("hit hit!"+imei);
					break;
				}
			}
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("finish");
	}
}
