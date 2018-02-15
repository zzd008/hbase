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
 * java����hbase API
 */
public class HbaseTest {

	/**
	 * ����ss ����ɾ����ʹ��admin������ɾ�Ĳ�������table����
	 */
	static Configuration config = null;
	private Connection connection = null;
	private Table table = null;

	/*
	 * ��ȡhbase�Ŀͻ��˽Ӷ���
	 */
	@Before
	public void init() throws Exception {
		config = HBaseConfiguration.create();// ����
		config.set("hbase.zookeeper.quorum", "master,slave1,slave2");// zookeeper��ַ
		config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper�˿�
		connection = ConnectionFactory.createConnection(config);
		table = connection.getTable(TableName.valueOf("user"));//Ҫ���ӵı�
	}

	/**
	 * ����һ����
	 * 
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {
		// �����������
		HBaseAdmin admin = new HBaseAdmin(config); // hbase�����
//		admin=(HBaseAdmin) connection.getAdmin();
		// ������
		TableName tableName = TableName.valueOf("test3"); 
		if(!admin.tableExists(tableName)){
			// ������������
			HTableDescriptor desc = new HTableDescriptor(tableName);
			// ���������������
			HColumnDescriptor family = new HColumnDescriptor("info"); // ����
			// ��������ӵ�����
			desc.addFamily(family);
			HColumnDescriptor family2 = new HColumnDescriptor("info2"); // ����
			// ��������ӵ�����
			desc.addFamily(family2);
			// ������
			admin.createTable(desc); // ������
		}else{
			System.out.println("���Ѿ����ڣ�");
		}
	}

	/*
	 * ɾ����
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
	 * ��hbase����������
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "deprecation", "resource" })
	@Test
	public void insertData() throws Exception {
		table.setAutoFlushTo(false);//�Ƿ��Զ�ˢ������
		table.setWriteBufferSize(534534534);
		ArrayList<Put> arrayList = new ArrayList<Put>();
		for (int i = 1; i < 3; i++) {
			//���ݷ�װ��
			Put put = new Put(Bytes.toBytes("zzd 1234"+i));//�м�
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"+i));//���� �� ֵ
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("password"), Bytes.toBytes(1234+i));
			arrayList.add(put);
		}
		
		/*
		 * һ��һ����ӣ�һ��put����һ�У��ܷ�װ�ܶ����ݣ��ܶ��У�
		 */
//		Put p=new Put(Bytes.toBytes("123456"));
//		p.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("zzd"));//��װput����
//		p.add(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("11"));//��װput����
//		p.add(Bytes.toBytes("info1"), Bytes.toBytes("phone"), Bytes.toBytes(234));//���ﲻҪ��int��234 ����hbase shell�л�ת����ʾ\x00\x00\x00\xEA ������������ַ�����"234"
//		table.put(p); //���һ������
		
		//����������� ��put����ŵ�������
		table.put(arrayList);
		//�ύ
		table.flushCommits();//ˢ�����ݲ��ύ
	}

	/**
	 * �޸�����
	 * 
	 * @throws Exception
	 */
	@Test
	public void uodateData() throws Exception {
		Put put = new Put(Bytes.toBytes("1234"));
		put.add(Bytes.toBytes("info1"), Bytes.toBytes("namessss"), Bytes.toBytes("lisi1234"));
		put.add(Bytes.toBytes("info1"), Bytes.toBytes("password"), Bytes.toBytes("123456"));
		//��������
		table.put(put);
		//�ύ
		table.flushCommits();
	}

	/**
	 * ɾ������
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteDate() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("123456"));//Ҫɾ������
//		table.delete(delete);//ɾ��һ��
//		delete.addFamily(Bytes.toBytes("info2"));//ɾ��ĳһ�����µ�����
		delete.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"));//ɾ��ĳһ�����µ�ĳһ���ڵĵ�Ԫ��
		table.delete(delete);
		table.flushCommits();
	}

	/**
	 * ������ѯ,��ȡһ�����ݰ汾
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void queryData() throws Exception {
		Get get = new Get(Bytes.toBytes("1234"));
		Result result = table.get(get);//ֱ�Ӳ��һ����������
		//��ѯĳһ�л�����
//		get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("namessss"));
//		get.addFamily(Bytes.toBytes("info1"));
		
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));//��result�л�ȡĳһ�����µ�ĳһ�е�����
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("namessss"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("sex"))));
		
//		List<KeyValue> column = result.getColumn(Bytes.toBytes("info1"), Bytes.toBytes("password"));

//		System.out.println(column);
		
//		result.getRow();//��ȡrowkey ����װ�����塢�С�������
		
//		result.rawCells();//��ȡ��һ����������������е�Ԫ��
		
//		List<Cell> cells = result.getColumnCells(Bytes.toBytes("info1"), Bytes.toBytes("password"));//�õ���Ԫ��
//		for(Cell c:cells){
//			c.getValue();//��ȡ��Ԫ���ֵ
//			c.getFamily();//�����Ԫ������塢����Ϣ
//		}
	}
	
	/**
	 * ������ѯ,��ȡһ����Ԫ����������ݰ汾
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void queryData1() throws Exception {
		Get get = new Get(Bytes.toBytes("1234"));
//		get.setMaxVersions(); //���ð汾��Ϊ this.maxVersions = Integer.MAX_VALUE;
		get.setMaxVersions(3);//����һ���Ի�ȡ���ٸ��汾������  
		get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("password"));
		Result result = table.get(get);
		
		for(Cell c:result.listCells()){
			System.out.println(Bytes.toString(c.getValue()));
		}
		
	}

	
	/**
	 * ȫ��ɨ�� ��ò�Ҫȫɨ��ָ��ĳһ��ɨ�ȽϿ�
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanData() throws Exception {
		Scan scan = new Scan();
		//scan.addFamily(Bytes.toBytes("info"));
		//scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
		scan.setStartRow(Bytes.toBytes("1234"));//������ʼ�������� ���Ӿ���ɨ��ȫ��
		scan.setStopRow(Bytes.toBytes("123456"));//������������123456����
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {//�����õ�ÿһ�е�result
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
//			result.rawCells();//��ȡ��һ����������������е�Ԫ��
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}
	}

	/**
	 * ȫ��ɨ��Ĺ�����
	 * ��ֵ������
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter1() throws Exception {

		// ����ȫ��ɨ���scan
		Scan scan = new Scan();
		//����������ֵ������ select *from user where info1.password=123456
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info1"),
				Bytes.toBytes("password"), CompareFilter.CompareOp.EQUAL, //CompareOp.EQUAL
				Bytes.toBytes("123456"));
		// ��scan���ù�����
		scan.setFilter(filter);

		// ��ӡ�����
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("---------"+Bytes.toString(result.getRow())+"----------");
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));//123456 toInt�ᳬ����Χ
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}

	}
	/**
	 * rowkey������  select *from user where rowkey startwith(12345)
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter2() throws Exception {
		
		// ����ȫ��ɨ���scan
		Scan scan = new Scan();
		//ƥ��rowkey������12345��ͷ��
		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^12345"));//^12345��12345��ͷ�� 
		// ���ù�����
		scan.setFilter(filter);
		// ��ӡ�����
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("---------"+Bytes.toString(result.getRow())+"----------");
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
			
		}

		
	}
	
	/**
	 * ƥ������ǰ׺
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter3() throws Exception {
		
		// ����ȫ��ɨ���scan
		Scan scan = new Scan();
		//ƥ��rowkey��name��ͷ����
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));//��name��ͷ������
		// ���ù�����
		scan.setFilter(filter);
		//Ҳ�������� ��scan��ֱ��ָ��������
//		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		// ��ӡ�����
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("---------"+Bytes.toString(result.getRow())+"----------");
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));//Ϊɶ��null��������������
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
			
		}
		
	}
	/**
	 * ����������
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter4() throws Exception {
		
		// ����ȫ��ɨ���scan
		Scan scan = new Scan();
		//���������ϣ�MUST_PASS_ALL��and�� �����������еĹ�����������ȫ��ͨ����,MUST_PASS_ONE(or�� �����������еĹ�����������ͨ��һ��)   
		//select password,name from user where rowkey ��12345��ͷ and password=1234;
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		//ƥ��rowkey��wangsenfeng��ͷ��
		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^12345"));//��12345��ͷ���м�
		//ƥ��password��ֵ����1234
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info1"),
				Bytes.toBytes("password"), CompareOp.EQUAL,
				Bytes.toBytes("1234"));
		filterList.addFilter(filter);
		filterList.addFilter(filter2);
		// ���ù�����
		scan.setFilter(filterList);
		// ��ӡ�����
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