package cn.zxw.hbase.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 建立HBase表
 */
public class CreateHBaseTable {

	@SuppressWarnings({"resource" })
	public static void main(String[] args) {
		if(args.length != 3){
			System.out.println("Arguments error!!!");
			return;
		}
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", args[0]);
		// 获取HBase表名
		String tableName = args[1];
		// 获取HBase列族名，多个列族以逗号分隔
		String columnFamilies = args[2];
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			String[] columnFamilyArr = columnFamilies.split(",");
			// 为表设置列族名称
			for(String columnFamily : columnFamilyArr){
				tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			}
			//设置预分区region的拆分范围
			byte[][] regions = new byte[][] { 
					Bytes.toBytes("11111111"), //
					Bytes.toBytes("22222222"), //
					Bytes.toBytes("33333333"), //
					Bytes.toBytes("44444444"), //
					Bytes.toBytes("55555555"), //
					Bytes.toBytes("66666666"), //
					Bytes.toBytes("77777777"), //
					Bytes.toBytes("88888888"), //
					Bytes.toBytes("99999999"), //
					Bytes.toBytes("aaaaaaaa"), //
					Bytes.toBytes("bbbbbbbb"), //
					Bytes.toBytes("cccccccc"), //
					Bytes.toBytes("dddddddd"), //
					Bytes.toBytes("eeeeeeee") //
			};
			hBaseAdmin.createTable(tableDescriptor, regions);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
