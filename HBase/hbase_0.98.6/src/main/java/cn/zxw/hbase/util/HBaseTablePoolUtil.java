package cn.zxw.hbase.util;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

@SuppressWarnings("deprecation")
public class HBaseTablePoolUtil {
	private static HTablePool tablePool=new HTablePool(HBaseConnectUtil.getConf(), 5);
	
	public static HTablePool getHTablePool(){
		return tablePool;
	}
	
	public static HTable getTable(String tableName) throws IOException {
		if(tableName==null){
			return null;
		}
		return (HTable) tablePool.getTable(tableName);
	}

	public static void close() throws IOException {
		System.out.println("finalize runing ...");
		if(tablePool!=null){
			tablePool.close();
		}
	}
}
