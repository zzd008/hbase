package cn.jxust.bigdata.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * java操作hbase API
 */
public class HbaseTest {

	/**
	 * 配置ss 创建删除表使用admin对象，增删改查数据用table对象
	 */
	static Configuration config = null;
	private Connection connection = null;
	private Table table = null;

	/*
	 * 获取hbase的客户端接对象
	 */
	@Before
	public void init() throws Exception {
		config = HBaseConfiguration.create();// 配置
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");// zookeeper地址
		config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		connection = ConnectionFactory.createConnection(config);
		table = connection.getTable(TableName.valueOf("user"));//要连接的表
	}

	/**
	 * 创建一个表
	 * 
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {
		// 创建表管理类
		HBaseAdmin admin = new HBaseAdmin(config); // hbase表管理
//		admin=(HBaseAdmin) connection.getAdmin();
		// 表名称
		TableName tableName = TableName.valueOf("test3"); 
		if(!admin.tableExists(tableName)){
			// 创建表描述类
			HTableDescriptor desc = new HTableDescriptor(tableName);
			// 创建列族的描述类
			HColumnDescriptor family = new HColumnDescriptor("info"); // 列族
			// 将列族添加到表中
			desc.addFamily(family);
			HColumnDescriptor family2 = new HColumnDescriptor("info2"); // 列族
			// 将列族添加到表中
			desc.addFamily(family2);
			// 创建表
			admin.createTable(desc); // 创建表
		}else{
			System.out.println("表已经存在！");
		}
	}

	/*
	 * 删除表
	 */
	@Test
	@SuppressWarnings("deprecation")
	public void deleteTable() throws MasterNotRunningException,
			ZooKeeperConnectionException, Exception {
		HBaseAdmin admin = new HBaseAdmin(config);
		admin.disableTable("test3");
		admin.deleteTable("test3");
		admin.close();
	}

	/**
	 * 向hbase中增加数据
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "deprecation", "resource" })
	@Test
	public void insertData() throws Exception {
		table.setAutoFlushTo(false);//是否自动刷新数据
		table.setWriteBufferSize(534534534);
		ArrayList<Put> arrayList = new ArrayList<Put>();
		for (int i = 1; i < 3; i++) {
			//数据封装类
			Put put = new Put(Bytes.toBytes("zzd 1234"+i));//行键
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"+i));//列族 列 值
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("password"), Bytes.toBytes(1234+i));
			arrayList.add(put);
		}
		
		/*
		 * 一条一条添加，一个put对象（一行）能封装很多数据（很多列）
		 */
//		Put p=new Put(Bytes.toBytes("123456"));
//		p.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("zzd"));//封装put对象
//		p.add(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("11"));//封装put对象
//		p.add(Bytes.toBytes("info1"), Bytes.toBytes("phone"), Bytes.toBytes(234));//这里不要用int的234 这样hbase shell中会转码显示\x00\x00\x00\xEA ，所以最好用字符串的"234"
//		table.put(p); //添加一条数据
		
		//插入多条数据 将put对象放到集合中
		table.put(arrayList);
		//提交
		table.flushCommits();//刷新数据并提交
	}

	/**
	 * 修改数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void uodateData() throws Exception {
		Put put = new Put(Bytes.toBytes("1234"));
		put.add(Bytes.toBytes("info1"), Bytes.toBytes("namessss"), Bytes.toBytes("lisi1234"));
		put.add(Bytes.toBytes("info1"), Bytes.toBytes("password"), Bytes.toBytes("123456"));
		//插入数据
		table.put(put);
		//提交
		table.flushCommits();
	}

	/**
	 * 删除数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteDate() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("123456"));//要删除的行
//		table.delete(delete);//删除一行
//		delete.addFamily(Bytes.toBytes("info2"));//删除某一列族下的数据
		delete.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"));//删除某一列族下的某一列在的单元格
		table.delete(delete);
		table.flushCommits();
	}

	/**
	 * 单条查询,获取一个数据版本
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void queryData() throws Exception {
		Get get = new Get(Bytes.toBytes("1234"));
		Result result = table.get(get);//直接查出一行所有数据
		//查询某一列或列族
//		get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("namessss"));
//		get.addFamily(Bytes.toBytes("info1"));
		
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));//从result中获取某一列族下的某一列的数据
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("namessss"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("sex"))));
		
//		List<KeyValue> column = result.getColumn(Bytes.toBytes("info1"), Bytes.toBytes("password"));

//		System.out.println(column);
		
//		result.getRow();//获取rowkey 它封装了列族、列。。。。
		
//		result.rawCells();//获取这一结果集所包含的所有单元格
		
//		List<Cell> cells = result.getColumnCells(Bytes.toBytes("info1"), Bytes.toBytes("password"));//得到单元格
//		for(Cell c:cells){
//			c.getValue();//获取单元格的值
//			c.getFamily();//这个单元格的列族、列信息
//		}
	}
	
	/**
	 * 单条查询,获取一个单元格的锁哥数据版本
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void queryData1() throws Exception {
		Get get = new Get(Bytes.toBytes("1234"));
//		get.setMaxVersions(); //设置版本数为 this.maxVersions = Integer.MAX_VALUE;
		get.setMaxVersions(3);//设置一次性获取多少个版本的数据  
		get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("password"));
		Result result = table.get(get);
		
		for(Cell c:result.listCells()){
			System.out.println(Bytes.toString(c.getValue()));
		}
		
	}

	
	/**
	 * 全表扫描 最好不要全扫，指定某一列扫比较快
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanData() throws Exception {
		Scan scan = new Scan();
		//scan.addFamily(Bytes.toBytes("info"));
		//scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
		scan.setStartRow(Bytes.toBytes("1234"));//设置起始、结束行 不加就是扫描全表
		scan.setStopRow(Bytes.toBytes("123456"));//不包含结束行123456！！
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {//遍历得到每一行的result
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
//			result.rawCells();//获取这一结果集所包含的所有单元格
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}
	}

	/**
	 * 全表扫描的过滤器
	 * 列值过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter1() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		//过滤器：列值过滤器 select *from user where info1.password=123456
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info1"),
				Bytes.toBytes("password"), CompareFilter.CompareOp.EQUAL, //CompareOp.EQUAL
				Bytes.toBytes("123456"));
		// 给scan设置过滤器
		scan.setFilter(filter);

		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("---------"+Bytes.toString(result.getRow())+"----------");
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));//123456 toInt会超出范围
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}

	}
	/**
	 * rowkey过滤器  select *from user where rowkey startwith(12345)
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter2() throws Exception {
		
		// 创建全表扫描的scan
		Scan scan = new Scan();
		//匹配rowkey等于以12345开头的
		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^12345"));//^12345以12345开头的 
		// 设置过滤器
		scan.setFilter(filter);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("---------"+Bytes.toString(result.getRow())+"----------");
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
			
		}

		
	}
	
	/**
	 * 匹配列名前缀
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter3() throws Exception {
		
		// 创建全表扫描的scan
		Scan scan = new Scan();
		//匹配rowkey以name开头的列
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));//以name开头的列名
		// 设置过滤器
		scan.setFilter(filter);
		//也可以这样 在scan中直接指定好类名
//		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("---------"+Bytes.toString(result.getRow())+"----------");
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));//为啥是null？？？？？？？
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
			
		}
		
	}
	/**
	 * 过滤器集合
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter4() throws Exception {
		
		// 创建全表扫描的scan
		Scan scan = new Scan();
		//过滤器集合：MUST_PASS_ALL（and与 过滤器集合中的过滤条件必须全部通过）,MUST_PASS_ONE(or或 过滤器集合中的过滤条件至少通过一条)   
		//select password,name from user where rowkey 以12345开头 and password=1234;
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		//匹配rowkey以wangsenfeng开头的
		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^12345"));//以12345开头的行键
		//匹配password的值等于1234
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info1"),
				Bytes.toBytes("password"), CompareOp.EQUAL,
				Bytes.toBytes("1234"));
		filterList.addFilter(filter);
		filterList.addFilter(filter2);
		// 设置过滤器
		scan.setFilter(filterList);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("---------"+Bytes.toString(result.getRow())+"----------");
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
		}
		
	}

	@After
	public void close() throws Exception {
		table.close();
		connection.close();
	}

}