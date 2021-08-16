package org.simple.store.cassandra;

import com.datastax.driver.core.Session;
import org.junit.BeforeClass;

import static org.junit.Assert.*;

import org.junit.Test;
import org.simple.store.*;
import org.simple.util.DynamicPool;

import java.io.*;

public class StoreCassandraTest {

    @BeforeClass
    public static void createStore() {

        storeFctry = new CassandraStoreFactory(9042, System.getProperty("cassandra.servers", "127.0.0.1"),
                TEST_STORE+ ".config");
        storeFctry.createStore(TEST_STORE);
        store = storeFctry.getStore(TEST_STORE);
        store.housekeeping();
        store.housekeeping();
    }

    private static CassandraStoreFactory storeFctry;
    private static Store store;

    public void update() {

        try {
            Store s = store;
            FileMetadata meta = new FileMetadata(TEST_STORE, test_key2, 1000, 1);
            s.insertMeta(meta);
            s.insertChunk(meta.id, 1, "abcdefg".getBytes());
            s.move(test_key2, "/tmp/1.jpg");
            s.readMetadata("/tmp/1.jpg");
            //s.restore(TEST_STORE, "/tmp/1.jpg");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void listDirs() {
        try {
            System.out.println(store.listDir("/").subdir.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void version() {
        CassandraStore cassStore = (CassandraStore) store;

        cassStore.cleanVersion(new ParentName("/home/johnny/", "1.gif"));
        try {
            Thread.sleep(60000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void lock() {
        CassandraStore cassStore = (CassandraStore) store;
        assertTrue(cassStore.cloudLock.lock("abc", "1", 600000));
        assertFalse(cassStore.cloudLock.trylock("abc", "2", 60));
        assertFalse(cassStore.cloudLock.lock("abc", "2", 1200));
        assertTrue(cassStore.cloudLock.lock("abc", "1", 1200));
        assertUnlock(cassStore.cloudLock, "abc", "2");
        cassStore.cloudLock.unlock("abc", "1");
        assertUnlock(cassStore.cloudLock, "cdf", "1");
    }

    private void assertUnlock(DistributedLock cloudLock, String obj, String owner) {
        try {
            cloudLock.unlock(obj, owner);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(cloudLock.failed_to_unlock));
        }
    }

    @Test
    public void config() throws IOException {
        Store.Config config = store.getConfig();
        assertTrue(config.DELETE_TTL == 2400);
        config = store.getConfig(); // verify no refresh
        assertTrue(config.INSERT_CHUNK_RETRY == 8);

        final int run_times=12;
        DynamicPool.PooledSession[] ps= new DynamicPool.PooledSession[run_times];
        for(int i=0; i< run_times; i++) {
            final int ii= i;
            new Thread(()->
            ps[ii] = storeFctry.pool.get()).start();
        }
        for(int i=0; i< run_times; i++) {
            final int ii= i;
            new Thread(()->{
                    try {
                        ps[ii].conn.close();
                    }
                    catch (Exception e){
                    }}).start();
        }
    }

    public void SplitTest() {
        final String key = "/home/johnny/1.mp4";
        try (FileInputStream fins = new FileInputStream(key);) {

            byte[] buf = new byte[8192];

            int len;
            int fileSize = 0;

            OutputStream cos = store.getOutputStream(key);


            while ((len = fins.read(buf)) != -1) {
                fileSize = fileSize + len;
                cos.write(buf, 0, len);
            }

            cos.close();
            System.out.println("fileSize:" + fileSize);


            StoreReader cre = (StoreReader) store.getReader(key, new FileOutputStream("/tmp/1.mp4"));
            assertEquals(cre.read(), fileSize);

            InputStream cins = store.getInputStream(key);
            FileOutputStream osm = new FileOutputStream("/tmp/2.mp4");
            int size = 0;

            while ((len = cins.read(buf)) != -1) {
                osm.write(buf, 0, len);
                size = size + len;
            }
            osm.close();
            assertEquals(fileSize, size);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public final static String TEST_STORE = "maven2";
    private final static String test_key2 = "/home/johnny/2.jpg";
}
