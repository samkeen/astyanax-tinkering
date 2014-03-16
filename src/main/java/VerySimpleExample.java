import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * see https://github.com/Netflix/astyanax/wiki/Getting-Started
 */
public class VerySimpleExample {

    private static final String CASSANDRA_VERSION = "1.2";
    private static final String CQL_VERSION = "3.1.8";
    private static final String CLUSTER_NAME = "Cluster0";
    private static final String KEYSPACE_NAME = "Keyspace0";
    private static final String CF_NAME = "Standard1";
    private static final String CONNECTION_POOL_NAME = "ConnectionPool0";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9160;

    /**
     * Running this example should result in:
     *
     * <code>
     *   cqlsh:Keyspace0Name> use "Keyspace0";
     *   cqlsh:Keyspace0> SELECT * FROM "Standard1";
     *
     *   key  | column1 | value
     *   ------+---------+-------
     *   4567 | Column1 |  0x58
     *   4567 | Column2 |  0x58
     *   4567 | Column3 |  0x58
     *   1234 | Column1 |  0x58
     *   1234 | Column2 |  0x58
     * </code>
     *
     * @param args
     */
    public static void main(String[] args) {
        Keyspace keyspace_client = getKeyspaceClient();
        try {
            ColumnFamily<String, String> CF_STANDARD1 = recreateEnvironment(keyspace_client);

            String rowKey1 = "1234";
            String rowKey2 = "4567";

            MutationBatch m = keyspace_client.prepareMutationBatch();

            // Batch a couple rows to add
            m.withRow(CF_STANDARD1, rowKey1)
                    .putColumn("Column1", "X", null)
                    .putColumn("Column2", "X", null);
            m.withRow(CF_STANDARD1, rowKey2)
                    .putColumn("Column1", "X", null)
                    .putColumn("Column2", "X", null)
                    .putColumn("Column3", "X", null);

            // now execute the batch
            m.execute();

        } catch (ConnectionException e) {
            e.printStackTrace();
        } finally {
            keyspace_client = null;
        }
    }

    /**
     * Initialize the Keyspace Client
     * @return
     */
    private static Keyspace getKeyspaceClient() {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setCqlVersion(CQL_VERSION)
                        .setTargetCassandraVersion(CASSANDRA_VERSION)
                )
                .forCluster(CLUSTER_NAME)
                .forKeyspace(KEYSPACE_NAME)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CONNECTION_POOL_NAME)
                        .setPort(PORT)
                        .setMaxConnsPerHost(1)
                        .setSeeds(HOST.concat(":").concat(String.valueOf(PORT)))
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        Keyspace keyspace = context.getClient();
        return keyspace;
    }

    /**
     * Recreate the Keyspace and ColumnFamily (in an idempotent manner)
     * @param keyspace
     * @return
     * @throws ConnectionException
     */
    private static ColumnFamily<String, String> recreateEnvironment(Keyspace keyspace) throws ConnectionException {
        recreateKeyspace(keyspace);
        ColumnFamily<String, String> CF_STANDARD1 =
                ColumnFamily.newColumnFamily(
                        CF_NAME,
                        StringSerializer.get(),
                        StringSerializer.get());
        keyspace.createColumnFamily(CF_STANDARD1, null);
        return CF_STANDARD1;
    }

    /**
     * Create the Keyspace.  Drop it first if it exists
     *
     * @param keyspace
     * @throws ConnectionException
     */
    private static void recreateKeyspace(Keyspace keyspace) throws ConnectionException {
        // first Drop if exists
        try {
            keyspace.dropKeyspace();
        } catch (BadRequestException e) {
            // simply means keyspace did not yet exist
        }
        // now add
        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class", "SimpleStrategy")
                .build()
        );
    }

}
