package org.simple.store.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.simple.util.LatestConfig;
import org.simple.store.Store;
import org.simple.store.StoreFactory;
import org.simple.util.DynamicPool;
import org.simple.util.LastModified;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraStoreFactory implements StoreFactory {

    public CassandraStoreFactory(int port, String servers, String pool_config_keyspace) {
        this.cluster = Cluster.builder().addContactPoints(servers.split(",")).withPort(port).build();
        this.pool = getDynamicPoolConfig(pool_config_keyspace);

        this.storeMap = new ConcurrentHashMap<>();
    }

    public DynamicPool getDynamicPoolConfig(String pool_config_keyspace) {
        return new DynamicPool<Session>(new LatestConfig<DynamicPool.Config>() {
            @Override
            protected LastModified<DynamicPool.Config> refresh(long lastModified) {
                Session session = cluster.newSession();
                Row r = session.execute("SELECT attr, blobAsBigint(timestampAsBlob(last_modified)) AS last_modified, poll_interval FROM " +
                        pool_config_keyspace + " WHERE name='DynamicPool' AND last_modified >" + lastModified +
                        " LIMIT 1 ALLOW FILTERING").one();
                session.close();
                if (r == null) { // lastModified is latest
                    return null;
                } else {
                    Map<String, String> map = r.getMap("attr", String.class, String.class);
                    return new LastModified<DynamicPool.Config>(new DynamicPool.Config(Integer.parseInt(map.get("min_conn")),
                            Integer.parseInt(map.get("max_conn")), Long.parseLong(map.get("max_busy_time")),
                            Long.parseLong(map.get("max_idle_time"))), r.getLong("last_modified"), r.getLong("poll_interval"));
                }
            }
        }) {
            @Override
            protected Session newObject() {
                return cluster.connect();
            }

            @Override
            protected boolean isClosed(Session obj) {
                return obj == null || obj.isClosed();
            }
        };
    }

    @Override
    public Store getStore(String name) {
        Store store = storeMap.get(name);
        if (store == null) {
            System.out.println("create store:" + name);
            store = new CassandraStore(pool, name);
            storeMap.put(name, store);
        }
        return store;
    }

    // parent, name, id are universal unique, so safe to insert without lock for many version, when delete, set
    @Override
    public void createStore(String name) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            pooled.conn.execute("CREATE KEYSPACE IF NOT EXISTS " + name + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            pooled.conn.execute("CREATE TABLE IF NOT EXISTS " + name + ".config (name TEXT, last_modified TIMESTAMP, attr MAP<TEXT, TEXT>, poll_interval BIGINT, PRIMARY KEY (name, last_modified)) WITH CLUSTERING ORDER BY (last_modified DESC)");
            pooled.conn.execute("INSERT INTO "+ name+ ".config (name, last_modified, attr, poll_interval) VALUES ('maven2', toTimeStamp(now()), {'chunk_size':'5242880', 'insert_chunk_retry':'8', 'delete_ttl':'2400', 'max_ver_size':'3'}, 600000)");
            pooled.conn.execute("INSERT INTO "+ name+ ".config (name, last_modified, attr, poll_interval) VALUES ('DynamicPool', toTimeStamp(now()), {'min_conn':'1', 'max_conn':'3', 'max_busy_time':'30000', 'max_idle_time':'60000'}, 120000)");
            pooled.conn.execute("CREATE TABLE IF NOT EXISTS " + name + ".metadata( parent TEXT, name TEXT, id UUID, size BIGINT, chunks INT, active BOOLEAN, PRIMARY KEY (parent, name, id))");
            pooled.conn.execute("CREATE INDEX IF NOT EXISTS meta_id ON " + name + ".metadata (id)");
            pooled.conn.execute("CREATE INDEX IF NOT EXISTS meta_active ON " + name + ".metadata (active)");
            pooled.conn.execute("CREATE TABLE IF NOT EXISTS " + name + ".chunks( id UUID, seq INT, data BLOB, PRIMARY KEY (id, seq)) WITH CLUSTERING ORDER BY (seq ASC)");
            pooled.conn.execute("CREATE TABLE IF NOT EXISTS " + name + ".dirs (parent text PRIMARY KEY, subdir SET<TEXT>, last_modified TIMESTAMP)");
        }
    }

    @Override
    public void deleteStore(String name) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            pooled.conn.execute("DROP KEYSPACE " + name);
        }
    }

    private ConcurrentHashMap<String, Store> storeMap;
    protected final DynamicPool<Session> pool;
    private final Cluster cluster;
}
