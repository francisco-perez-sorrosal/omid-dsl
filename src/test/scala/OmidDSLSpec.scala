package com.fps.omid.dsl.hbase

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.omid.transaction._
import org.scalatest.{BeforeAndAfter, FunSpecLike, Inside, Matchers}

import scala.util.{Failure, Success}

class OmidDSLSpec extends DefaultOmidSuite with FunSpecLike with BeforeAndAfter with Matchers with Inside {

  val testLogger = Logger(classOf[OmidDSLSpec])

  before {

  }

  after {

  }

  describe("The Omid DSL") {

    it("should commit a correct basic transaction") {

      createAppTable("users_table", Array("data", "club"))
      createAppTable("my_club_member_table", Array("client_data"))
      createUserTableRow

      val tmDSL = new HBaseTxManager {

        // Realization of the HBase configuration value using the minicluster conf
        implicit val hbaseConf: Configuration = hbaseCluster.getConfiguration
        // Realization of the omidTm using the instance created in the Omid test suite
        implicit val omidTm: TransactionManager = omidTxMgr

        import HBaseDataConversions._

        (HBaseTxBuilder

          define transaction

            GET("getClient", "users_table")
            PUT("insertMember", "my_club_member_table")

          body { context =>

            context getOp "getClient" from_row "id" + getClientId column ("data", "name") column ("club", "my_club")

            context executeDbOp "getClient" match {
              case Success(res) =>
                testLogger.info("Get client succeeded: {}. Res: {}", context, res)
                (context getOp "insertMember" to_row (generateClubId(getClientId))
                                              column ("client_data", "name", res.getValue("data", "name")))
                context executeDbOp "insertMember" match {
                  case Success(_) =>
                    testLogger.info("Member will be added to club: {}", context)
                  case Failure(ex) =>
                    testLogger.info("Can't add member to table", ex)
                }
              case Failure(ex) =>
                testLogger.info("Can't get client from DB", ex)
            }

          }).execute shouldBe TxResult.Committed


        verifyValue("my_club_member_table", "myclub1", "client_data", "name", "Francisco") shouldBe true

        deleteTable("users_table")
        deleteTable("my_club_member_table")

      }

    }

    it("should rollback a transaction explicitly set to rollback") {

      createAppTable("users_table", Array("data", "club"))

      val tmDSL = new HBaseTxManager {

        // Realization of the HBase configuration value using the minicluster conf
        implicit val hbaseConf: Configuration = hbaseCluster.getConf
        // Realization of the omidTm using the instance created in the Omid test suite
        implicit val omidTm: TransactionManager = omidTxMgr

        import HBaseDataConversions._

        (HBaseTxBuilder

          define transaction

            GET("getClient", "users_table")

          body { context =>

            context getOp "getClient" from_row "non-existing-id" column ("non-existing-fam", "name")

            context executeDbOp "getClient" match {
              case Success(res) =>
                if (res.isEmpty) {
                  context set_rollback
                } else {
                  fail(s"Get client succeeded: $context. Res: $res")
                }
              case Failure(ex) =>
                testLogger.info("Can't get client from DB", ex)
          }

        }).execute shouldBe TxResult.RolledBack

      }

      deleteTable("users_table")

    }

  }

  def getClientId = {
    1
  }

  def generateClubId(baseId: Int) = {
    s"myclub$baseId"
  }

}
