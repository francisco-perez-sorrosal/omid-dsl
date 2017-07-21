package com.fps.omid.dsl.hbase

import com.fps.omid.dsl.hbase.HBaseTxManager.DefineHelper
import com.fps.omid.dsl.hbase.OpType.OpType
import com.fps.omid.dsl.hbase.TxResult.TxResult
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Get => HBaseGet, Put => HBasePut}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.omid.committable.hbase.{HBaseCommitTable, HBaseCommitTableConfig}
import org.apache.omid.transaction.{HBaseOmidClientConfiguration, HBaseSyncPostCommitter, HBaseTransactionManager, RollbackException, TTable, TransactionException, TransactionManager, TransactionManagerException, Transaction => OmidTransaction}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

// https://nami.me/2015/09/30/building-a-simple-and-performant-dsl-in-scala/

object transaction // Fake object for parameterless begin method

object TxResult extends Enumeration {
  type TxResult = Value
  val Unknown, Committed, RolledBack, Error = Value
}

class TransactionWrapper(implicit omidTm: TransactionManager, hbaseConfig: Configuration) {

  implicit val txContext = new HBaseTransactionalContext
  val corpus = new DefineHelper()

}

abstract class TransactionalContext {

  val logger = Logger(classOf[TransactionalContext])

  val operations = mutable.HashMap[String, Operation]()
  val tables = mutable.HashMap[String, TTable]()

  var omidTx: Option[OmidTransaction] = None
  var result = TxResult.Unknown

  def state = Try(omidTx.getOrElse(throw new IllegalStateException("No Omid Tx!")).getStatus)

  def getOp(name: String): GetOrPutOrNoOp

  def executeDbOp(name: String): Try[Result]

  def set_rollback(): Unit = {
    omidTx match {
      case Some(tx) =>
        tx.setRollbackOnly()
      case None =>
        logger.warn("Trying to rollback a transaction that hasn't started yet")
    }
  }

  override def toString = {

    s"($omidTx, $tables, $operations, $result)"

  }

}

object GetOrPutOrNoOp {
  implicit def fromGet(g: Get): GetOrPutOrNoOp = new GetOrPutOrNoOp(Some(g), None, None)
  implicit def fromPut(p: Put): GetOrPutOrNoOp = new GetOrPutOrNoOp(None, Some(p), None)
  implicit def fromNoOp(n: NoOp.type): GetOrPutOrNoOp = new GetOrPutOrNoOp(None, None, Some(n))
  implicit def GetOrPutorNoOpToGet(v: GetOrPutOrNoOp): Get = v.g.get
  implicit def GetOrPutorNoOpToPut(v: GetOrPutOrNoOp): Put = v.p.get
  implicit def GetOrPutorNoOpToNoOp(v: GetOrPutOrNoOp): NoOp.type = NoOp
}

case class GetOrPutOrNoOp(g: Option[Get], p: Option[Put], n: Option[NoOp.type])

class HBaseTransactionalContext extends TransactionalContext {

  override def getOp(name: String): GetOrPutOrNoOp = {

    logger.info("Operations size: {}", operations.keySet.toString())

    operations.get(name) match {
      case Some(result) =>
        result.opType match {
          case OpType.Get =>
            logger.info("Get")
            result.asInstanceOf[Get]
          case OpType.Put =>
            logger.info("Put")
            result.asInstanceOf[Put]
          case OpType.NoOp =>
            logger.info("NoOp")
            NoOp
        }
      case None =>
        logger.info("None")
        NoOp
    }

  }

  override def executeDbOp(name: String): Try[Result] = {

    logger.info("Executing op {}", name)
    getOp(name) match {
      case GetOrPutOrNoOp(Some(g), None, None) =>
        logger.info("Executing get op {}", g)
        g.execute(this)
      case GetOrPutOrNoOp(None, Some(p), None) =>
        logger.info("Executing put op {}", p)
        p.execute(this)
      case _ =>
        logger.warn("Executing nothing!")
        Try(new Result())
    }

  }

}

object OpType extends Enumeration {
  type OpType = Value
  val Get, Put, NoOp = Value
}

trait Operation {

  val logger = Logger(classOf[Operation])

  val name: String
  val opType: OpType
  def execute(tx: TransactionalContext): Try[Result]
}

trait HBaseOperation extends Operation {
  var r: Array[Byte] = Array()
  val columns = new ListBuffer[(Array[Byte], Array[Byte])]
}

object NoOp extends HBaseOperation {

  override val name: String = "NoOp"
  override val opType: OpType = OpType.NoOp

  override def execute(tx: TransactionalContext): Try[Result] = {
    /** Do Nothing */
    Success(new Result)
  }

}

case class Get(val name: String, val tableName: String) extends HBaseOperation {

  override val opType: OpType = OpType.Get

  def from_row(rowId: Array[Byte]): Get = {
    r = rowId
    this
  }

  def column(colDef: (Array[Byte], Array[Byte])): Get = {
    columns += ((colDef))
    this
  }

  override def execute(txContext: TransactionalContext): Try[Result] = {
    val txGet = prepare
    txContext.tables.get(tableName) match {

      case Some(table) =>
        logger.info("some {}", tableName)
        Try(table.get(txContext.omidTx.getOrElse(throw new IllegalStateException("No Omid Tx!")), txGet)) match {
          case Success(result) =>
            logger.info("success! {}", result)
            Success(result)
          case Failure(ex) =>
            txContext.omidTx.get.setRollbackOnly()
            Failure(ex)
        }
      case None =>
        logger.info("no table {}", tableName)
        txContext.omidTx.get.setRollbackOnly()
        Failure(new IllegalStateException("Table " + tableName + " not found in Tx Context"))
    }
  }

  def prepare: HBaseGet = {
    val txGet = new HBaseGet(r)
    columns.foreach(c => txGet.addColumn(c._1, c._2))
    txGet
  }

  override def toString: String ={
    s"$opType $name ($tableName, ${Bytes.toString(r)}, ${columns.map(c => (Bytes.toString(c._1), Bytes.toString(c._2)))}"
  }

}

class Put(val name: String, val tableName: String) extends HBaseOperation {

  override val opType: OpType = OpType.Put

  val values = new ListBuffer[Array[Byte]]

  def to_row(rowId: Array[Byte]): Put = {
    r = rowId
    this
  }

  def column(colDef: (Array[Byte], Array[Byte], Array[Byte])): Put = {
    logger.info("Adding fam:col:val {}", colDef)
    columns += ((colDef._1, colDef._2))
    values += colDef._3
    this
  }

  override def execute(txContext: TransactionalContext): Try[Result] = {
    val txPut = prepare
    txContext.tables.get(tableName) match {
      case Some(table) =>
        Try(table.put(txContext.omidTx.getOrElse(throw new IllegalStateException("No Omid Tx!")), txPut)) match {
          case Success(_) =>
            Success(new Result)
          case Failure(ex) =>
            txContext.omidTx.get.setRollbackOnly()
            Failure(ex)
        }
      case None =>
        txContext.omidTx.get.setRollbackOnly()
        Failure(new IllegalStateException("Table " + tableName + " not found in Tx Context"))
    }
  }

  def prepare: HBasePut = {
    val txPut = new HBasePut(r)
    (columns zip values) map (c => txPut.add(c._1._1, c._1._2, c._2))
    txPut
  }

  override def toString: String ={
    s"$opType $name ($tableName, ${Bytes.toString(r)}, ${columns.map(c => (Bytes.toString(c._1), Bytes.toString(c._2)))}, ${values.map(c => Bytes.toString(c))}"
  }

}

object HBaseTxBuilder {
  def define(ops: transaction.type)(implicit omidTM: TransactionManager, hbaseConf: Configuration) = new TransactionWrapper().corpus
}

trait HBaseTxManager {

  implicit val hbaseConf: Configuration
  implicit val omidTm: TransactionManager
//  = {
//    val clientConf = new HBaseOmidClientConfiguration
//    clientConf.setHBaseConfiguration(this.hbaseConf)
//    HBaseTransactionManager.newInstance(clientConf)
//  }

}

object HBaseTxManager {

  class DefineHelper()(implicit txContext: TransactionalContext, omidTM: TransactionManager, hbaseConf: Configuration) {

    val logger = Logger(classOf[DefineHelper])

    def GET(name: String, tableName: String) = {
      txContext.tables.get(tableName) match {
        case None =>
          logger.info("Creating instance of table {} for Tx {}", tableName, txContext)
          txContext.tables.put(tableName, new TTable(hbaseConf, tableName))
      }
      txContext.operations.get(name) match {
        case Some(op) =>
          logger.warn("An operation named {} already exists ({}). Doing nothing...", (name, op))
        case None =>
          val op = new Get(name, tableName)
          logger.info("Creating operation {}", op)
          txContext.operations.put(name, op)
      }
      this
    }

    def PUT(name: String, tableName: String) = {
      txContext.tables.get(tableName) match {
        case None =>
          logger.info("Creating instance of table {} for Tx {}", tableName, txContext)
          txContext.tables.put(tableName, new TTable(hbaseConf, tableName))
      }
      txContext.operations.get(name) match {
        case Some(op) =>
          logger.warn("An operation named {} already exists ({}). Doing nothing...", (name, op))
        case None =>
          val op = new Put(name, tableName)
          logger.info("Creating operation {}", op)
          txContext.operations.put(name, op)
      }
      this
    }

    def body(f: TransactionalContext => Unit) = {
      Try(omidTM.begin()) match {
        case Success(omidTx) =>
          logger.info("Transaction started: {}", omidTx)
          txContext.omidTx = Some(omidTx)
          f(txContext)
        case Failure(ex) =>
          logger.error("Error starting tx: {}", ex)
      }
      new CompleteHelper
    }
  }

  class CompleteHelper()(implicit txContext: TransactionalContext, omidTM: TransactionManager) {

    val logger = Logger(classOf[CompleteHelper])

    def execute(): TxResult = {

      txContext.omidTx match {

        case None =>
          logger.error("There's no Omid TX to commit! for context {}", txContext)
          TxResult.Error

        case Some(tx) =>
          logger.info("Committing {}", tx)
          if (tx.isRollbackOnly) {
            Try(omidTM.rollback(tx)) match {
              case Success(_) =>
                TxResult.RolledBack
              case Failure(ex) =>
                logger.error("Error on rolling-back tx {}: {}", txContext, ex)
                TxResult.Error
            }
          } else {
            Try(omidTM.commit(tx)) match {
              case Success(_) =>
                logger.info("{} COMMITTED", tx)
                TxResult.Committed
              case Failure(ex) => {
                logger.info("{} Failed", tx)
                ex match {
                  case ex: RollbackException =>
                    TxResult.RolledBack
                  case ex: TransactionException =>
                    logger.error("Error on committing tx {}: {}", txContext, ex)
                    TxResult.Error
                  case ex: TransactionManagerException =>
                    logger.error("Error on committing tx {}: {}", txContext, ex)
                    TxResult.Error
                }
              }
            }
          }

      }

    }
  }

}



