package cn.zxw.hbase.table;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import cn.zxw.hbase.constant.Constants;
import cn.zxw.hbase.util.HBaseConnectUtil;
import cn.zxw.hbase.util.MyTestCase;

/**
 * 表操作
 * @author zhangxw
 */
public class TableOpera extends MyTestCase{
	
	public void testCreateTable() throws IOException {
		if(HBaseConnectUtil.getAdmin().tableExists(Constants.TB_NAME_OBJ)){
			System.out.println("表已存在");
		}else{
			HTableDescriptor table = new HTableDescriptor(Constants.TB_NAME_OBJ);
			HColumnDescriptor family=new HColumnDescriptor(Constants.CF_DEFAULT)
							.setCompressionType(Algorithm.SNAPPY)//设置列簇压缩
							.setMaxVersions(HConstants.ALL_VERSIONS);//版本数
			table.addFamily(family);
			HBaseConnectUtil.getAdmin().createTable(table);
			System.out.println("表创建成功");
		}
	}
	
	public void testDeleteTable() throws IOException {
		HBaseConnectUtil.getAdmin().disableTable(Constants.TB_NAME_OBJ);
		HBaseConnectUtil.getAdmin().deleteTable(Constants.TB_NAME_OBJ);
		System.out.println("删除表成功！");
	}
	
	public void testDescTable(String tableName) throws IOException {
		HTableDescriptor htd = HBaseConnectUtil.getTable(tableName).getTableDescriptor();
		HColumnDescriptor[] hcds = htd.getColumnFamilies();
		for(HColumnDescriptor hcd : hcds){
			System.out.println(hcd.getNameAsString());
		}
	}
	
	public void testAlterTable() throws IOException {
		HTableDescriptor htd = new HTableDescriptor(Constants.TB_NAME_OBJ); 
		htd.setReadOnly(true);
		HBaseConnectUtil.getAdmin().modifyTable(Constants.TABLE_NAME, htd);
		
		HColumnDescriptor family=new HColumnDescriptor(Constants.CF_DEFAULT)
								.setCompressionType(Algorithm.SNAPPY)//设置列簇压缩
								.setMaxVersions(HConstants.ALL_VERSIONS);//版本数
		HBaseConnectUtil.getAdmin().modifyColumn(Constants.TABLE_NAME, family);
		System.out.println("修改表成功！");
	}
}
