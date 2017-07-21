package org.apache.omid.tso

import java.net.{SocketException, UnknownHostException}
import javax.inject.Singleton

import com.google.common.base.Preconditions
import com.google.inject.name.Named
import com.google.inject.{AbstractModule, Provider, Provides}
import org.apache.hadoop.conf.Configuration
import org.apache.omid.committable.hbase.{DefaultHBaseCommitTableStorageModule, HBaseCommitTable, HBaseCommitTableStorageModule}
import org.apache.omid.committable.{CommitTable, InMemoryCommitTable}
import org.apache.omid.metrics.{MetricsRegistry, NullMetricsProvider}
import org.apache.omid.timestamp.storage.{DefaultHBaseTimestampStorageModule, HBaseTimestampStorage, HBaseTimestampStorageModule, TimestampStorage}
import org.apache.omid.tso.LeaseManagement.LeaseManagementException

class TSOMockModule(val config: TSOServerConfig, val hbaseConfig: Configuration) extends AbstractModule {
  Preconditions.checkArgument(config.getNumConcurrentCTWriters >= 2)

  protected def configure() {
    bind(classOf[Configuration]).toInstance(hbaseConfig)
    bind(classOf[TSOChannelHandler]).in(classOf[Singleton])
    bind(classOf[TSOStateManager]).to(classOf[TSOStateManagerImpl]).in(classOf[Singleton])
    bind(classOf[CommitTable]).to(classOf[HBaseCommitTable]).in(classOf[Singleton])
    bind(classOf[TimestampStorage]).to(classOf[HBaseTimestampStorage]).in(classOf[Singleton])
    bind(classOf[TimestampOracle]).to(classOf[TimestampOracleImpl]).in(classOf[Singleton])
    bind(classOf[Panicker]).to(classOf[MockPanicker]).in(classOf[Singleton])
    install(new BatchPoolModule(config))
    install(new DisruptorModule())
  }

  @Provides private[tso] def provideTSOServerConfig = config

  @Provides
  @Singleton private[tso] def provideMetricsRegistry: MetricsRegistry = new NullMetricsProvider

  @Provides
  @Named("tso.hostandport")
  @throws[SocketException]
  @throws[UnknownHostException]
  private[tso] def provideTSOHostAndPort = NetworkInterfaceUtils.getTSOHostAndPort(config)

  @Provides
  @Singleton
  @throws[LeaseManagementException]
  private[tso] def provideLeaseManager(tsoChannelHandler: TSOChannelHandler, stateManager: TSOStateManager): LeaseManagement =
    new VoidLeaseManager(tsoChannelHandler, stateManager)

  @Provides
  private[tso] def getPersistenceProcessorHandler(provider: Provider[PersistenceProcessorHandler]) = {

    val persistenceProcessorHandlers = new Array[PersistenceProcessorHandler](config.getNumConcurrentCTWriters)
    var i = 0
    while (i < persistenceProcessorHandlers.length) {

      persistenceProcessorHandlers(i) = provider.get
      i += 1

    }
    persistenceProcessorHandlers
  }
}
