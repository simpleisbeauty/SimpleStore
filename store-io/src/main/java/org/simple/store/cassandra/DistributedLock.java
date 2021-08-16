package org.simple.store.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.simple.util.DynamicPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLock {

    public DistributedLock(DynamicPool pool, String keyspace) {
        this.pool = pool;
        this.keyspace = keyspace;
        logger = LoggerFactory.getLogger(getClass());
    }

    // lock will expire after ttl_second
    public boolean trylock(String obj, String owner, int lock_ttl_sec) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            ResultSet rs = pooled.conn.execute("UPDATE " + keyspace + ".lock USING TTL " + lock_ttl_sec +
                    " SET owner='" + owner + "' WHERE obj='" + obj + "' IF owner=null");
            if (rs.wasApplied()) {
                return true;
            } else {
                String curr_owner = rs.one().getString(1);
                logger.warn(owner + " to lock " + obj + " already locked by " + curr_owner);
                return owner.equals(curr_owner);
            }
        }
    }

    public void unlock(String obj, String owner) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {

            ResultSet rs = pooled.conn.execute("UPDATE " + keyspace + ".lock SET owner=null WHERE obj='" + obj + "' IF owner='" +
                    owner + "'");
            if (!rs.wasApplied()) {
                Row r= rs.one();
                String curr_owner= r.getColumnDefinitions().size() > 1 ? r.getString(1): null;
                throw new RuntimeException(owner + failed_to_unlock + obj + " by " + curr_owner);
            }
        }
    }

    // lock auto expire after 1 hours
    public boolean lock(String obj, String owner, long timeout_milli){
        return this.lock(obj, owner, timeout_milli, 3600, 500);
    }

    public boolean lock(String obj, String owner, long timeout_milli, int lock_ttl_sec,
                        long wait_interval_milli) {

        long retry = timeout_milli/wait_interval_milli + 1;
        for(int i=0; i< retry; i++) {
            if (trylock(obj, owner, lock_ttl_sec)) {
                return true;
            } else {
                try {
                    logger.info(retry + "/"+ i+ ": waiting "+ wait_interval_milli+ " millis to try again");
                    Thread.sleep(wait_interval_milli);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }
        return false;
    }

    public final String failed_to_unlock= " failed to unlock ";

    private final DynamicPool pool;
    private final String keyspace;
    private final Logger logger;
}
