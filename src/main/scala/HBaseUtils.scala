import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

import scala.util.{Failure, Success, Try}

/**
  * HBase 工具类
  *
  * @author zhanghao
  *
  */
object HBaseUtils {

  /*
   *创建一个HBase的配置，创建的时候会去加载classpath下的hbase-default.xml和hbase-site.xml两个配置文件
   */
  private val conf = HBaseConfiguration.create()
  //设置Zookeeper的地址和端口来访问HBase，先从配置中读取，如配置中不存在，设置地址为localhost，端口为默认端口2181
  conf.set(HConstants.ZOOKEEPER_QUORUM, conf.get(HConstants.ZOOKEEPER_QUORUM, HConstants.LOCALHOST))
  conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, conf.get(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT.toString))


  //创建操作HBase的入口connection
  private val conn: Connection = ConnectionFactory.createConnection(conf)
  //创建一个Admin来操作表
  private val admin: Admin = conn.getAdmin


  /**
    * 获取Admin
    *
    * @return Admin
    */
  def getAdmin: Admin = {
    admin
  }

  /**
    * 获取表
    *
    * @param tableName 表名
    * @return HBase表
    */

  def getTable(tableName: String): Table = {
    val table = Try(conn.getTable(TableName.valueOf(tableName)))
    table.get.close()
    table match {
      case Success(v) => v;
      case Failure(e) => e.printStackTrace()
        null
    }
  }


  /**
    * 创建表
    *
    * @param tableName 表名
    * @param cf        列族
    */
  def createTable(tableName: String, cf: String): Unit = {
    //创建表
    val tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
    tableDesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("basic".getBytes).build())
    println(s"Creating table `$tableName`. ")
    Try {
      if (admin.tableExists(TableName.valueOf(tableName))) {
        admin.disableTable(TableName.valueOf(tableName))
        admin.deleteTable(TableName.valueOf(tableName))
      }
      admin.createTable(tableDesc.build())
      admin.close()
      println("Done!")
    } match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace()
    }

  }


  /**
    *
    * 往表中存放数据
    *
    * @param tableName 表名
    * @param rowKey    行键
    * @param cf        列族
    * @param qualifier 列限定符
    * @param value     具体的值
    */
  def put(tableName: String, rowKey: String, cf: String, qualifier: String, value: String): Unit = {
    println(s"Put row key $rowKey into $tableName. ")
    val table = conn.getTable(TableName.valueOf(tableName))
    Try {
      //准备一个row key
      val p = new Put(rowKey.getBytes)
      //为put操作指定 column qualifier 和 value
      p.addColumn(cf.getBytes, qualifier.getBytes, value.getBytes)
      //放数据到表中
      table.put(p)
      table.close()
    } match {
      case Success(_) => println("Done!")
      case Failure(e) => e.printStackTrace()
    }
  }


  /**
    * 删除表
    *
    * @param tableName 表名
    * @param rowKey    行键
    */
  def delete(tableName: String, rowKey: String): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    Try {
      val d = new Delete(rowKey.getBytes)
      table.delete(d)
      table.close()
    } match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace()
    }

  }


  /**
    * 获得表里面的数据
    *
    * @param tableName 表名
    * @param rowKey    行键
    * @param cf        列族
    * @param qualifier 列限定符
    * @return 获得的数据
    */
  def get(tableName: String, rowKey: String, cf: String, qualifier: String): String = {
    val table = conn.getTable(TableName.valueOf(tableName))
    Try {
      val g = new Get(rowKey.getBytes)
      val result = table.get(g)
      table.close()
      Bytes.toString(result.getValue(cf.getBytes(), qualifier.getBytes()))
    } match {
      case Success(v) => v
      case Failure(e) => e.printStackTrace()
        null
    }

  }


  /**
    * 扫描数据
    *
    * @param tableName 表名
    * @param cf        列族
    * @param qualifier 列限定符
    */
  def scan(tableName: String, cf: String, qualifier: String): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val s = new Scan()
    s.addColumn(cf.getBytes, qualifier.getBytes)
    val scanner = table.getScanner(s)
    Try {
      val iterator = scanner.iterator()
      while (iterator.hasNext) {
        val next = iterator.next()
        println("Found row: " + next)
        println("Found value: " + Bytes.toString(
          next.getValue(cf.getBytes, qualifier.getBytes)))
      }
      scanner.close()
      table.close()
    } match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace()
    }

  }


  def main(args: Array[String]): Unit = {
    get("user", "u001", "basic", "lily")
  }


}
