# Simple Scala DSL for Omid Transactions

Usage:

```scala

import com.fps.omid.dsl.hbase._
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.omid.transaction.HBaseOmidClientConfiguration
import org.apache.omid.transaction.HBaseTransactionManager
import org.apache.omid.transaction.TransactionManager

import scala.util.{Failure, Success}

object BasicTransaction extends App with HBaseTxManager {

  val logger = Logger("BasicTransaction")
  
  // Realization of the HBase configuration value using the minicluster conf
  implicit val hbaseConf: Configuration = HBaseConfiguration.create
  // Realization of the omidTm using the instance created in the Omid test suite
  implicit val omidTm: TransactionManager = {
    val clientConf = new HBaseOmidClientConfiguration
    clientConf.setHBaseConfiguration(this.hbaseConf)
    HBaseTransactionManager.newInstance(clientConf)
  }

  (HBaseTxBuilder

    define transaction

      GET("getClient", "users_table")
      PUT("changeName", "my_club_member_table")

    body { context =>

      (context getOp "getClient" from_row Bytes.toBytes("id1")
                                 column (Bytes.toBytes("data"), Bytes.toBytes("name"))
                                 column (Bytes.toBytes("club"), Bytes.toBytes("my_club")))

      context executeDbOp("getClient") match {
        case Success(res) =>
          (context getOp "insertMember" to_row Bytes.toBytes("myclub1")
                                        column (Bytes.toBytes("client_data"), Bytes.toBytes("name"),
                                        res.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))))
          context executeDbOp("insertMember")  match {
            case Success(_) =>
              logger.info("Member will be added to club: {}", context)
            case Failure(ex) =>
              logger.info("Can't add member to table", ex)
          }
        case Failure(ex) =>
          logger.info("Can't get client from DB", ex)
      }
    }).execute

}

```

