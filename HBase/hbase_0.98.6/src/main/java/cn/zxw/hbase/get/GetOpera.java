package cn.zxw.hbase.get;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import cn.zxw.hbase.constant.Constants;
import cn.zxw.hbase.util.HBaseConnectUtil;
import cn.zxw.hbase.util.MyTestCase;

public class GetOpera extends MyTestCase{
	
	public void testGet(){
		try {
			getValue(Constants.TABLE_NAME,Constants.ROW_KEY_001,Constants.CF_DEFAULT,Constants.COL_USER);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Result getRow(String tableName,String rowKey) throws IOException {
		HTable table=HBaseConnectUtil.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		Result res = table.get(get);
		return res;
	}
	
	public String getValue(String tableName,String rowKey,String family,String qualifier) throws IOException {
		HTable table=HBaseConnectUtil.getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		byte[] b = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		String res=new String(b);
		System.out.println(res);
		return res;
	}
	
	public void getFamilyVersions(String tbName,String rowKey,String family,String...columns) throws IOException{
		Get get=new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(family));
		get.setMaxVersions();//所有版本
		Result rs = HBaseConnectUtil.getTable(tbName).get(get);
		if(rs!=null){
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map=rs.getMap();//family,column,datas
			if(map!=null){
				NavigableMap<byte[], NavigableMap<Long, byte[]>> cfAllVersions=map.get(Bytes.toBytes(family));//column,datas
				if(cfAllVersions!=null){
					for(String column:columns){
						NavigableMap<Long, byte[]> versions=cfAllVersions.get(Bytes.toBytes(column));//timestamp,data
						for(Long version:versions.keySet()){
							System.out.println(version+","+new String(versions.get(version)));//print
						}
					}
				}
			}
		}
	}
	
	
}
