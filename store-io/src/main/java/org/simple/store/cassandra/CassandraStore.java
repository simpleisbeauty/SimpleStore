package org.simple.store.cassandra;


import org.simple.util.DynamicPool;
import org.simple.util.LastModified;
import org.simple.store.*;
import org.simple.util.LatestConfig;
import org.simple.util.chunk.ChunkReader;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.utils.UUIDs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import java.util.concurrent.ForkJoinPool;


public class CassandraStore implements Store {

    public CassandraStore(DynamicPool<Session> pool, String name) {
        this.pool = pool;
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            bstatMeta = pooled.conn.prepare("INSERT INTO " + name +
                    ".metadata (parent, name, id, size, chunks, active) VALUES (:parent, :name, :id,  :size, :chunks, :active)").bind();
            bstatChunk = pooled.conn.prepare("INSERT INTO " + name +
                    ".chunks (id, seq, data) VALUES (:id, :seq, :data) IF NOT EXISTS").bind(); // prevent UUID collision
            bstatDir = pooled.conn.prepare("INSERT INTO " + name +
                    ".dirs (parent, subdir, last_modified) VALUES (:parent, :subdir, toTimeStamp(now()))").bind();
        }
        this.name = name;

        this.config = new LatestConfig<Config>() {
            @Override
            protected LastModified<Config> refresh(long lastModified) {
                try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
                    Row r = pooled.conn.execute("SELECT attr, blobAsBigint(timestampAsBlob(last_modified)) AS last_modified, poll_interval FROM " +
                            name + ".config WHERE name='" + name + "' AND last_modified >" + lastModified +
                            " LIMIT 1 ALLOW FILTERING").one();
                    if (r == null) { // lastModified is latest
                        return null;
                    } else {
                        Map<String, String> map = r.getMap("attr", String.class, String.class);
                        return new LastModified<>(new Config(Integer.parseInt(map.get(chunk_size)),
                                Integer.parseInt(map.get(delete_ttl)), Integer.parseInt(map.get(insert_chunk_retry)),
                                Integer.parseInt(map.get(max_ver_size))),
                                r.getLong("last_modified"), r.getLong("poll_interval"));
                    }
                }
            }
        };
        cloudLock = new DistributedLock(pool, name);
        logger = LoggerFactory.getLogger(getClass());

    }


    @Override
    public void delete(String key) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            ParentName pn = FileMetadata.getParentAndName(key);
            ResultSet rs = pooled.conn.execute("SELECT id, active FROM " +
                    name + ".metadata WHERE parent= '" + pn.parent + "' and name = '" + pn.name + "'");
            StringBuilder sb = new StringBuilder("UPDATE ").append(name).append(".metadata USING TTL ").
                    append(config.getConfig().DELETE_TTL).append(" set active=false WHERE parent='").
                    append(pn.parent).append("' and name='").append(pn.name).append("' and id in ( ");

            for (Row r : rs) {
                if (r.getBool(active)) {
                    sb.append(r.getUUID("id")).append(',');
                }
            }
            sb.setCharAt(sb.length() - 1, ')');
            pooled.conn.execute(sb.toString());
            delDirTree(pooled.conn, pn.parent);
            logger.debug("delete: " + key);
        }
    }

    // recursively delete all (file and sub dir) in the parent folder
    @Override
    public void deleteRecursive(String parent) {
        listDir(parent).subdir.forEach(d -> deleteRecursive(parent + d));
        listFile(parent).forEach(m -> deleteVersion(m.pn, m.id));
    }

    @Override
    public void restore(String key) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            ParentName pn = FileMetadata.getParentAndName(key);
            ResultSet rs = pooled.conn.execute("SELECT id, active FROM " +
                    name + ".metadata WHERE parent= '" + pn.parent + "' and name = '" + pn.name + "'");
            StringBuilder sb = new StringBuilder("UPDATE ").append(name).append(".metadata USING TTL 0 set active=true WHERE parent='").
                    append(pn.parent).append("' and name='").append(pn.name).append("' and id in ( ");

            for (Row r : rs) {
                if (!r.getBool(active)) {
                    sb.append(r.getUUID("id")).append(',');
                }
            }
            sb.setCharAt(sb.length() - 1, ')');
            pooled.conn.execute(sb.toString());
            addDirTree(pooled.conn, pn.parent);
            logger.debug("restored:" + key);
        }
    }

    @Override
    public List<FileMetadata> listDeleted(String parent) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            ResultSet rs = pooled.conn.execute("SELECT parent, name, id, size, writetime(size) as created_time, chunks, active FROM " +
                    name + ".metadata WHERE active=false");
            ArrayList<FileMetadata> deleted = new ArrayList<>();
            for (Row r : rs) {
                ParentName pn = new ParentName(r.getString("parent"), r.getString("name"));
                if (pn.parent.startsWith(parent) && !r.getBool(active)) {
                    deleted.add(new FileMetadata(name, pn, r.getLong("size"),
                            r.getInt("chunks"), r.getUUID("id"), false,
                            r.getTime("created_time")));
                }
            }
            return deleted;
        }
    }

    @Override
    public Config getConfig() {
        return config.getConfig();
    }

    @Override
    public void restoreRecursive(String parent) {

        listDeleted(parent).forEach(d -> {
            restoreVersion(d.pn, d.id);
        });
    }

    @Override
    public void deleteVersion(ParentName pn, UUID id) {
        if (pn == null)
            return;
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            pooled.conn.execute("UPDATE " + name + ".metadata USING TTL " + config.getConfig().DELETE_TTL +
                    " SET active=False WHERE parent='" + pn.parent + "' and name='" + pn.name + "' and id=" + id + " IF EXISTS");
            delDirTree(pooled.conn, pn.parent); // reuse conn, otherwise 2 conn
        }
    }

    @Override
    public void restoreVersion(ParentName pn, UUID id) {
        if (pn== null) return;

        final String log_msg = name + " restore ver:" + pn + +':' + id;
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            if (pooled.conn.execute("UPDATE " + name + ".metadata USING TTL 0 SET active=True WHERE parent='" +
                    pn.parent + "' and name='" + pn.name + "' and id=" + id + " IF EXISTS").wasApplied()) {
                logger.debug(log_msg);
                addDirTree(pooled.conn, pn.parent); // reuse conn
            }
        } catch (Exception e) {
            logger.error(log_msg, e);
        }
    }

    @Override
    public void trash(ParentName pn, UUID id) {
        ForkJoinPool.commonPool().submit(() -> {
            String log_msg = name + " pruge chunks: " + id;
            try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
                pooled.conn.execute("DELETE FROM " + name + ".metadata WHERE parent='" + pn.parent +
                        "' and name='" + pn.name + "' and id=" + id);
                if (pooled.conn.execute("SELECT name FROM " + name + ".metadata WHERE id=" + id + " LIMIT 1").one() == null) {
                    pooled.conn.execute("DELETE FROM " + name + ".chunks WHERE id = " + id);
                    logger.debug(log_msg);
                }

            } catch (Exception e) {
                logger.error(log_msg, e);
            }
        });
    }

    @Override
    public void moveVersion(String key1, UUID id, String key2) {
        if(copyVersion(key1, id, key2)){
            ParentName pn1= null;
            try{
                pn1 = FileMetadata.getParentAndName(key1);
                deleteVersion(pn1, id);
            } catch (Exception e) {
                restoreVersion(pn1, id);
                deleteVersion(FileMetadata.getParentAndName(key2), id);
                logger.error(key1 + " move version " + id + " to " + key2 + " failed, rollback", e);
            }
        }
    }

    @Override
    public void move(String key1, String key2) {

        if (key1.equals(key2)) {
            throw new IllegalArgumentException("can't move same source and destination " + key1);
        }
        if (cloudLock.lock(key1, key2, 600000)) {

            try {
                ParentName pn = FileMetadata.getParentAndName(key1);

                DynamicPool<Session>.PooledSession pooled = pool.get();
                ResultSet rs = pooled.conn.execute("SELECT id, size, chunks, active FROM " +
                        name + ".metadata WHERE parent= '" + pn.parent + "' and name = '" + pn.name + "'");
                pooled.close(); // return conn immediately for 1 conn in the flow
                for (Row r : rs) {
                    UUID id = r.getUUID("id");
                    if (r.isNull(active) || !r.getBool(active)) { // TTL expired or delete
                        trash(pn, id);
                    } else {
                        ParentName pn2 = FileMetadata.getParentAndName(key2);
                        copy(key1, key2);
                        delete(key1);
                        logger.debug(pn + " move version succeeded:" + id + ',' + pn2);
                    }
                }
            } catch (Exception e) {
                //clean up
                restore(key1);
                delete(key2);
                logger.error(key1 + " move to " + key2 + " failed, rollback", e);
            } finally {
                cloudLock.unlock(key1, key2);
            }
        }
    }

    @Override
    public void moveRecursive(String parent1, String parent2) {
        if (parent1.equals(parent2)) {
            throw new IllegalArgumentException("can't move same source and destination " + parent1);
        }
        listDir(parent1).subdir.forEach(d -> moveRecursive(parent1 + d, parent2 + d));
        listFile(parent1).forEach(m -> move(m.pn.key(), ParentName.key(parent2, m.pn.name)));

    }

    @Override
    public boolean copyVersion(String key1, UUID id, String key2) {
        if (key1.equals(key2)) {
            throw new IllegalArgumentException("can't copy same source and destination " + key1);
        }
        ParentName pn2 = null;
        boolean ret= true;
        try {
            FileMetadata meta = readMetadata(key1, id);
            pn2 = FileMetadata.getParentAndName(key2);
            insertMeta(new FileMetadata(meta.store, pn2, meta.size, meta.chunks,
                    meta.id, true, 0));// no need to set at insert
        } catch (Exception e) {
            ret= false;
            deleteVersion(pn2, id);
            logger.error(key1 + " copy version " + id + " to " + key2 + " failed, rollback", e);
        }
        return ret;
    }

    @Override
    public void copy(String key1, String key2) {

        if (key1.equals(key2)) {
            throw new IllegalArgumentException("same source and destination " + key1);
        }
        try {
            ParentName pn = FileMetadata.getParentAndName(key1);
            DynamicPool<Session>.PooledSession pooled = pool.get();
            ResultSet rs = pooled.conn.execute("SELECT id, size, chunks, active FROM " +
                    name + ".metadata WHERE parent= '" + pn.parent + "' and name = '" + pn.name + "'");
            pooled.close();
            for (Row r : rs) {
                UUID id = r.getUUID("id");
                if (r.isNull(active) || !r.getBool(active)) { // TTL expired or delete
                    trash(pn, id);
                } else {
                    ParentName pn2 = FileMetadata.getParentAndName(key2);
                    insertMeta(new FileMetadata(name, pn2, r.getLong("size"),
                            r.getInt("chunks"), id, true, 0));
                    logger.debug("copy version success: " + name + key1 + " to " + key2 + " id:" + id);

                }
            }
        } catch (Exception e) {// clean up
            delete(key2);
            logger.error(key1 + " copy to " + key2 + " failed, rollback", e);
        }
    }

    @Override
    public void copyRecursive(String parent1, String parent2) {
        if (parent1.equals(parent2)) {
            throw new IllegalArgumentException("can't copy same source and destination " + parent1);
        }
        listDir(parent1).subdir.forEach(d -> copyRecursive(parent1 + d, parent2 + d));
        listFile(parent1).forEach(m -> copy(m.pn.key(), ParentName.key(parent2, m.pn.name)));
    }

    // read latest version of the key
    @Override
    public StoreInputStream getInputStream(String key) throws FileNotFoundException {
        return new StoreInputStream(ForkJoinPool.commonPool(), readMetadata(key), this);
    }

    // read specific version of the key
    @Override
    public StoreInputStream getInputStream(String key, UUID id) throws FileNotFoundException {
        return new StoreInputStream(ForkJoinPool.commonPool(), readMetadata(key, id), this);
    }

    @Override
    public StoreOutputStream getOutputStream(String key) {
        return new StoreOutputStream(config.getConfig().CHUNK_SIZE, ForkJoinPool.commonPool(),
                new FileMetadata(name, key, 0, 0), this);
    }

    @Override
    public ChunkReader getReader(String key, OutputStream dest) throws FileNotFoundException {
        return new StoreReader(ForkJoinPool.commonPool(), readMetadata(key), dest, this);
    }

    @Override
    public ChunkReader getReader(UUID id, int chunks, OutputStream dest) throws FileNotFoundException {
        return new StoreReader(ForkJoinPool.commonPool(), new FileMetadata(name, null,
                0, chunks, id, true, 0), dest, this);
    }

    @Override
    public DirMeta listDir(String parent) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            Row r = pooled.conn.execute("SELECT subdir, last_modified FROM " + name + ".dirs WHERE parent='" + parent + "'").one();
            return r == null ? new DirMeta(new HashSet<>(), 0) :
                    new DirMeta(new HashSet<>(r.getSet("subdir", String.class)), r.getTimestamp("last_modified").getTime() * 1000);
        }
    }

    @Override
    public List<FileMetadata> listFile(String parent, boolean showDeleted) {
        ArrayList<FileMetadata> files = new ArrayList<>();
        try {
            DynamicPool<Session>.PooledSession pooled = pool.get();
            ResultSet rs = pooled.conn.execute("SELECT name, id, size, writetime(size) as created_time, chunks, active FROM " +
                    name + ".metadata WHERE parent='" + parent + "'");
            pooled.close();
            for (Row r : rs) {
                boolean activ = r.getBool(active);
                UUID id = r.getUUID("id");
                ParentName pn = new ParentName(parent, r.getString("name"));
                if (activ || showDeleted) {
                    files.add(new FileMetadata(name, pn, r.getLong("size"), r.getInt("chunks"), id, activ,
                            r.getTime("created_time")));
                }
            }
        } catch (Exception e) {
            logger.error("list dir:" + parent, e);
        }
        return files;
    }

    @Override
    public List<FileMetadata> listFile(String parent) {
        return listFile(parent, false);
    }


    // call every insert/restore metadata, reuse Session
    private void addDirTree(Session session, String parent) {
        boolean loop = true;

        String lock_id = UUIDs.timeBased().toString();
        while (loop) {
            ParentName pn = FileMetadata.getParentAndName(parent);
            parent = pn.parent;

            if (cloudLock.lock(parent, lock_id, 600000)) {
                if (listDir(parent).subdir.size() > 0) { // traverse before
                    loop = false;
                }
                //pn.name= subdir - could already exist in parent, make sure last_modified updated
                session.execute("UPDATE " + name + ".dirs SET last_modified=toTimestamp(now()), subdir= subdir + {'" +
                        pn.name + "'} WHERE parent='" + parent + "'");
                cloudLock.unlock(parent, lock_id);
            } else {
                throw new RuntimeException(parent + " lock failed to add dir tree");
            }

            if (parent.equals("/")) {
                loop = false;
            }
        }
    }

    // call only at trash() - purged
    private void delDirTree(Session session, String parent) {
        String lock_id = UUIDs.timeBased().toString();
        while (true) {
            String lock_obj = parent; // parent will change in loop
            if (cloudLock.lock(parent, lock_id, 600000)) {
                try {
                    if (!parent.equals("/") && !hasActiveFile(session, parent)
                            && listDir(parent).subdir.size() == 0) {
                        ParentName pn = FileMetadata.getParentAndName(parent);
                        // delete subdirs of current dir, and delete it from its parent
                        session.execute("BEGIN BATCH DELETE FROM " + name + ".dirs WHERE parent='" + parent +
                                "'; UPDATE " + name + ".dirs SET last_modified=toTimestamp(now()), subdir= subdir - {'" + pn.name +
                                "'} WHERE parent='" + pn.parent + "'; APPLY BATCH");
                        parent = pn.parent;
                        logger.debug("delete dir tre:" + parent + " by:" + lock_id);
                    } else {
                        break;
                    }
                } finally {
                    cloudLock.unlock(lock_obj, lock_id);
                }
            } else {
                throw new RuntimeException(parent + " lock failed to del dir tree");
            }
        }
    }

    private boolean hasActiveFile(Session session, String parent) {
        Row r = session.execute("SELECT active FROM " + name + ".metadata WHERE parent='" +
                parent + "' AND active=true LIMIT 1").one();
        return r == null ? false : true;
    }


    @Override
    public byte[] readChunk(UUID id, int seq) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {

            Row row = pooled.conn.execute("SELECT data FROM " + name + ".chunks WHERE id = " + id + " and seq=" + seq).one();
            return row.getBytes("data").array();
        } catch (Exception e) {
            return null;
        }
    }


    @Override
    public void insertChunk(UUID id, int seq, byte[] data) throws IOException {

        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            bstatChunk.setUUID("id", id);
            bstatChunk.setInt("seq", seq);
            bstatChunk.setBytes("data", ByteBuffer.wrap(data));
            ResultSet rs = pooled.conn.execute(bstatChunk);
            if (!rs.wasApplied()) {
                throw new IOException("insertChunk Error: " + name + "/" + id + ":" + seq);
            }
        }
    }

    // get latest write active version
    @Override
    public FileMetadata readMetadata(String key) throws FileNotFoundException {

        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            ParentName pn = FileMetadata.getParentAndName(key);
            ResultSet rs = pooled.conn.execute("SELECT id, size, writetime(size) as created_time, chunks, active FROM " +
                    name + ".metadata WHERE parent= '" + pn.parent + "' and name = '" + pn.name + "' AND active=true");
            long latest_created = 0;
            Row latest_row = null;
            UUID latest_id = null;
            for (Row r : rs) {
                UUID id = r.getUUID("id");
                long t = r.getTime("created_time");
                if (latest_created < t) { // later wrote
                    latest_created = t;
                    latest_id = id;
                    latest_row = r;
                }
            }
            if (latest_id == null) {
                throw new FileNotFoundException("not found:" + key);
            }
            return new FileMetadata(name, pn, latest_row.getLong("size"),
                    latest_row.getInt("chunks"), latest_id, true, latest_created);
        }
    }

    // can read inactive file version
    private FileMetadata readMetadata(String key, UUID id) throws FileNotFoundException {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            ParentName pn = FileMetadata.getParentAndName(key);
            Row r = pooled.conn.execute("SELECT size, writetime(size) as created_time, chunks FROM " +
                    name + ".metadata WHERE parent= '" + pn.parent + "' and name = '" + pn.name + "' AND id=" + id).one();
            if (r == null) {
                throw new FileNotFoundException("not found:" + key + " id:" + id);
            }
            return new FileMetadata(name, pn, r.getLong("size"),
                    r.getInt("chunks"), id, true, r.getTime("created_time"));
        }
    }

    @Override
    public void insertMeta(FileMetadata meta) throws IOException {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            insertMeta(pooled.conn, meta);
        }
    }

    @Override
    public void housekeeping() {

        final String lock_id = UUIDs.timeBased().toString();
        SingletonTask[] tsks = new SingletonTask[]{
                new SingletonTask("trash_purged_"+ name, cloudLock, 43200) {
                    @Override
                    protected void process() {
                        trashPurged();
                    }
                },
                new SingletonTask("clean_dangling_chunks_"+ name, cloudLock, 43200) {
                    @Override
                    protected void process() {
                        cleanDanglingChunks();
                    }
                },
                new SingletonTask("check_dir_tree_"+ name, cloudLock, 43200) {
                    @Override
                    protected void process() {
                        checkDirTree(lock_id);
                    }
                }
        };

        for (SingletonTask tsk : tsks) {
            ForkJoinPool.commonPool().submit(tsk);
        }

    }

    /**
     * e.g. {"/": ["home/", "dev/"], "/home/": ["johnny/", "tom/"], "/home/johnny/" :["download"]}
     **/
    private void checkDirTree(String lock_id) {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            ResultSet rs = pooled.conn.execute("SELECT DISTINCT parent FROM " + name + ".metadata");
            HashMap<String, Set<String>> actualDirMap = new HashMap<>();
            for (Row r : rs) {
                // check or add to dir set if not exists
                String parent = r.getString("parent");
                // distinct parent may have all inactive files
                if (hasActiveFile(pooled.conn, parent)) {
                    boolean loop = true;
                    while (loop) {
                        ParentName pn = FileMetadata.getParentAndName(parent);
                        parent = pn.parent;
                        Set subdir = actualDirMap.get(parent);
                        if (subdir == null) {
                            subdir = new HashSet<String>();
                            actualDirMap.put(parent, subdir);
                        } else if (subdir.size() > 0) {
                            loop = false;
                        }
                        //pn.name= subdir - could already exist in parent 
                        subdir.add(pn.name);
                        if (parent.equals("/")) {
                            loop = false;
                        }
                    }
                    //addDirTree(pooled.obj, parent);
                }
            }
            //comare actual dir tree with cached
            HashMap<String, Set<String>> cacheDirMap = new HashMap<>();
            rs = pooled.conn.execute("SELECT parent, subdir FROM " + name + ".dirs");
            for (Row r : rs) {
                Set<String> cacheSubdir = r.getSet("subdir", String.class);
                String parent = r.getString("parent");
                Set<String> actualSubdir = actualDirMap.get(parent);
                if (actualSubdir == null) {// need to remove from cached
                    logger.debug(parent + " remove " + cacheSubdir.toString());
                    if (cloudLock.lock(parent, lock_id, 600000)) {
                        pooled.conn.execute("DELETE FROM " + name + ".dirs WHERE parent='" + parent + "'");
                        cloudLock.unlock(parent, lock_id);
                    }
                    continue;
                }
                if (!actualSubdir.equals(cacheSubdir)) {
                    logger.debug(parent + " update " + cacheSubdir.toString() + " with " +
                            actualSubdir.toString());
                    updateSubdir(pooled.conn, lock_id, parent, actualSubdir);
                }
                cacheDirMap.put(parent, cacheSubdir);
            }
            actualDirMap.entrySet().stream().forEach(ent -> {
                String ke = ent.getKey();
                if (cacheDirMap.get(ke) == null) {// need to insert
                    logger.debug(ke + " not in cached, add " + ent.getValue());
                    updateSubdir(pooled.conn, lock_id, ke, ent.getValue());
                }
            });
        }

    }

    private void updateSubdir(Session session, String lock_id, String parent, Set<String> subdir) {

        bstatDir.setString("parent", parent);
        bstatDir.setSet("subdir", subdir);

        try {
            if (cloudLock.lock(parent, lock_id, 600000)) {
                session.execute(bstatDir);
                cloudLock.unlock(parent, lock_id);
            }
        } finally {
            cloudLock.unlock(parent, lock_id);
        }
    }

    // heavy
    private void trashPurged() {
        try {
            DynamicPool<Session>.PooledSession pooled = pool.get();
            ResultSet rs = pooled.conn.execute("SELECT parent, name, id, active FROM " + name + ".metadata ");
            pooled.close();
            for (Row r : rs) {
                String parent = r.getString("parent");
                if (r.isNull(active)) {
                    trash(new ParentName(parent, r.getString("name")), r.getUUID("id"));
                }
            }
        } catch (Exception e) {
            logger.error("trash purged", e);
        }
    }

    private void cleanDanglingChunks() {
        try (DynamicPool<Session>.PooledSession pooled = pool.get()) {
            ResultSet rs = pooled.conn.execute("SELECT DISTINCT id FROM " + name + ".chunks ");
            for (Row r : rs) {
                UUID id = r.getUUID("id");
                if (pooled.conn.execute("SELECT id FROM " + name + ".metadata WHERE id=" + id + " LIMIT 1 ").one() == null) {
                    logger.debug("Delete dangling Chunk id:" + id);
                    pooled.conn.execute("DELETE FROM " + name + ".chunks WHERE id = " + id);
                }
            }
        }
    }

    private void insertMeta(Session session, FileMetadata meta) throws IOException {

        bstatMeta.setString("parent", meta.pn.parent);
        bstatMeta.setString("name", meta.pn.name);
        bstatMeta.setUUID("id", meta.id);
        bstatMeta.setLong("size", meta.size);
        bstatMeta.setInt("chunks", meta.chunks);
        bstatMeta.setBool(active, meta.active);

        if (!session.execute(bstatMeta).wasApplied()) {
            throw new IOException("insertMetadata Error: " + meta.store + "/" + meta.pn.toString() + ":" + meta.id);
        }
        // no async, dir must create when file inserted, session isn't thread safe
        addDirTree(session, meta.pn.parent);
        cleanVersion(meta.pn);

    }

    // async delete extra versions
    protected void cleanVersion(ParentName pn) {
        ForkJoinPool.commonPool().submit(() -> {
            try {
                DynamicPool<Session>.PooledSession pooled = pool.get();
                ResultSet rs = pooled.conn.execute("SELECT id, writetime(size) as created_time FROM " + name +
                        ".metadata WHERE parent= '" + pn.parent + "' and name = '" + pn.name + "' AND active=true");
                pooled.close();
                HashMap<Long, UUID> ids = new HashMap<>();
                for (Row r : rs) {
                    ids.put(r.getLong("created_time"), r.getUUID("id"));
                }
                int max_ver = getConfig().MAX_VER_SIZE;
                if (max_ver < ids.size()) {
                    Long[] tms = ids.keySet().toArray(new Long[ids.size()]);
                    Arrays.sort(tms); // ascending

                    for (int i = 0; i < tms.length - max_ver; i++) {
                        UUID id = ids.get(tms[i]);
                        logger.debug(pn + ": clean ver:" + id + " time:" + tms[i]);
                        deleteVersion(pn, id);

                    }
                }
            } catch (Exception e) {
                logger.error(pn + " clean version", e);
            }
        });
    }

    public final static String active = "active";
    public final static String chunk_size = "chunk_size";
    public final static String delete_ttl = "delete_ttl";
    public final static String insert_chunk_retry = "insert_chunk_retry";
    public final static String max_ver_size = "max_ver_size";

    protected final DistributedLock cloudLock;
    protected final DynamicPool<Session> pool;
    protected final LatestConfig<Config> config;

    private final String name; // store name

    private final BoundStatement bstatDir;
    private final BoundStatement bstatMeta;
    private final BoundStatement bstatChunk;

    private final Logger logger;

}

