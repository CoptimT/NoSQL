package cn.zxw.hbase.admin;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import cn.zxw.hbase.util.HBaseConnectUtil;
import cn.zxw.hbase.util.MyTestCase;

public class AdminOpera extends MyTestCase{
	
	public void testListTables() throws IOException{
		HBaseAdmin admin=HBaseConnectUtil.getAdmin();
		TableName[] tbns=admin.listTableNames();
		for(TableName tbn:tbns){
			System.out.println(tbn.getNameAsString());
		}
	}
	
}
