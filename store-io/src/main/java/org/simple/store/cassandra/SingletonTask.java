package org.simple.store.cassandra;

import com.datastax.driver.core.utils.UUIDs;

public abstract class SingletonTask implements Runnable {

    public SingletonTask(String name, DistributedLock cloudLock,
                         int lock_ttl_sec){
        this.cloudLock= cloudLock;
        this.name= name;
        this.lock_ttl= lock_ttl_sec<=0? 1: lock_ttl_sec; // lock_ttl= 0: no lock expiration
    }

    @Override
    public void run() {
        String lock_id= UUIDs.timeBased().toString();
        if (cloudLock.trylock(name, lock_id, lock_ttl)) {
            try {
                process();
            }
            catch (Exception e){
                e.printStackTrace();
                cloudLock.unlock(name, lock_id); // must unlock when fail
            }
        }
    }
    protected abstract void process();

    final private DistributedLock cloudLock;
    public final String name;
    public final int lock_ttl;

}
