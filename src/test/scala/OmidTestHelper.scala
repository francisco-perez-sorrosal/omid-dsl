package org.apache.omid.transaction

import java.io.IOException
import java.net.Socket

import com.google.inject.Guice
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.hbase.client.{Get, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.omid.committable.hbase.{HBaseCommitTable, HBaseCommitTableConfig}
import org.apache.omid.tools.hbase.OmidTableManager
import org.apache.omid.tso.{TSOMockModule, TSOServer, TSOServerConfig}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait DefaultOmidSuite extends Suite with BeforeAndAfterAll {

  val logger = Logger(classOf[DefaultOmidSuite])

  val hbaseTestUtils = new HBaseTestingUtility()
  val hbaseCluster: MiniHBaseCluster = startHBase
  createOmidTables

  val omidInjector = Guice.createInjector(new TSOMockModule(new TSOServerConfig(), hbaseCluster.getConf))
  val tsoServer: TSOServer = startTSO

  var omidTxMgr: TransactionManager = _

  override def beforeAll(): Unit = {
    omidTxMgr = newTransactionManager()
  }

  override def afterAll() {
    tsoServer.stopAndWait()
    hbaseCluster.shutdown()
  }

  def createOmidTables: Unit = {
    logger.info("Creating Omid tables...")
    Set(Array[String]("commit-table", "-numRegions", "1"),
      Array[String]("timestamp-table")).foreach(args => createOmidTable(args))
  }


  @throws[IOException]
  private def createOmidTable(args: Array[String]) {
    val omidTableManager = new OmidTableManager(args:_*)
    omidTableManager.executeActionsOnHBase(hbaseCluster.getConf)
  }

  def startHBase: MiniHBaseCluster = {
    logger.info("Creating HBase minicluster")
    hbaseTestUtils.startMiniCluster(1)
  }

  def startTSO: TSOServer = {
    logger.info("Starting TSO")
    val tsoServer = omidInjector.getInstance(classOf[TSOServer])
    tsoServer.startAndWait
    waitForSocketListening("localhost", 54758, 100)
    tsoServer
  }

  @throws[IOException]
  def createAppTable(tableName: String, colFam: Array[String]) {
    logger.info("****************************************************************************************************")
    logger.info("Creating app table {}", tableName)
    logger.info("****************************************************************************************************")
    val admin = hbaseTestUtils.getHBaseAdmin
    val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
    colFam.foreach(fam => {
      val datafam = new HColumnDescriptor(fam)
      datafam.setMaxVersions(Integer.MAX_VALUE)
      tableDesc.addFamily(datafam)
    })
    admin.createTable(tableDesc)
  }

  @throws[IOException]
  def deleteTable(tableName: String) {
    val admin = hbaseTestUtils.getHBaseAdmin
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
  }

  def createUserTableRow() = {

    val txTable = new TTable(hbaseCluster.getConf, "users_table")
    val tx = omidTxMgr.begin()
    val row1 = new Put(Bytes.toBytes("id1"))
    row1.add(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("Francisco"))
    row1.add(Bytes.toBytes("club"), Bytes.toBytes("my_club_id"), Bytes.toBytes("fps"))
    txTable.put(tx, row1)
    omidTxMgr.commit(tx)

    logger.info("Setup Transaction {} ", tx)
  }

  def verifyValue(tableName: Array[Byte], row: Array[Byte], fam: Array[Byte], col: Array[Byte], value: Array[Byte]) = {
    val table = new HTable(hbaseCluster.getConf, tableName)
    try {
      val g = new Get(row).setMaxVersions(1)
      val r = table.get(g)
      val cell = r.getColumnLatestCell(fam, col)
      logger.info("Value for {}:{}:{}:{} => {} (expected {})", Bytes.toString(tableName),
        Bytes.toString(row),
        Bytes.toString(fam),
        Bytes.toString(col),
        Bytes.toString(CellUtil.cloneValue(cell)),
        Bytes.toString(value))
      Bytes.equals(CellUtil.cloneValue(cell), value)

    } catch {

      case e: IOException => {
        logger.error("Error reading row " + Bytes.toString(tableName) + ":" + Bytes.toString(row) + ":" + Bytes.toString(fam) + Bytes.toString(col), e)
        false
      }

    } finally if (table != null) table.close()
  }

  @throws[Exception]
  def newTransactionManager(): TransactionManager = {
    logger.info("Creating new tx mgr")
    val clientConf = new HBaseOmidClientConfiguration
    clientConf.setHBaseConfiguration(hbaseCluster.getConfiguration)
    HBaseTransactionManager.builder(clientConf).build
  }

  @throws[IOException]
  @throws[InterruptedException]
  def waitForSocketListening(host: String, port: Int, sleepTimeMillis: Int) {

    var sock: Socket = null
    while (true) {

      try {
        sock = new Socket(host, port)
        if (sock != null) return
      } catch {
        case e: IOException => Thread.sleep(sleepTimeMillis)
      }
    }

  }


}
