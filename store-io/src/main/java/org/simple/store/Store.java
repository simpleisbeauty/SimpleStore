package org.simple.store;

import org.simple.util.chunk.ChunkReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;


// key = dir + name
public interface Store {

    //parent = xxx/, unique list
    static HashMap<String, List<String>>  getDirTree(List<String> parents){
        HashMap<String, List<String>> dirs = new HashMap<>();
        for(String parent: parents) {
            boolean loop = true;
            while (loop) {
                ParentName pn = FileMetadata.getParentAndName(parent);
                parent= pn.parent;
                List<String> subs = dirs.get(parent);
                if (subs == null) { // no traverse before
                    subs = new ArrayList<>();
                    subs.add(pn.name);
                    dirs.put(parent, subs);

                } else { // traversed before
                    if(!subs.contains(pn.name)) {
                        subs.add(pn.name);
                    }
                    loop = false;
                }
                if (parent.equals("/")) {
                    loop = false;
                }
            }
        }
        return dirs;
    }



    //soft delete all version of the key, set active=false TTL
    void delete(String key);
    // revert the soft delete
    void restore(String key);
    // soft delete all of the parent folder
    void deleteRecursive(String parent);
    // restore all of the parent folder
    void restoreRecursive(String parent);

    //soft delete one version, set active=false TTL
    void deleteVersion(ParentName pn, UUID id);
    // restore the soft deleted version
    void restoreVersion(ParentName pn, UUID id);

    //hard delete after active TTL
    void trash(ParentName pn, UUID id);

    void moveVersion(String key1, UUID id, String key2);
    void move(String key1, String key2);
    void moveRecursive(String parent1, String parent2);

    // copy == hard link, no soft link available
    boolean copyVersion(String key1, UUID id, String key2);
    void copy(String key1, String key2);
    void copyRecursive(String parent1, String parent2);

    StoreInputStream getInputStream(String key) throws FileNotFoundException;
    // shortcut to read stream by id and chunk size directly
    StoreInputStream getInputStream(String key, UUID id) throws FileNotFoundException;

    StoreOutputStream getOutputStream(String key);

    ChunkReader getReader(String key, OutputStream dest) throws FileNotFoundException;
    // shortcut to read to dest stream by id and chunk size directly
    ChunkReader getReader(UUID id, int chunks, OutputStream dest) throws FileNotFoundException;

    FileMetadata readMetadata(String key) throws FileNotFoundException;
    void insertMeta(FileMetadata meta) throws IOException;

    void insertChunk(UUID id, int seq, byte[] data) throws IOException;
    // return null if seq not exists, can be used for random text file reader
    byte[] readChunk(UUID id, int seq);

    DirMeta listDir(String parent);

    // list dir files with showDeleted filter
    List<FileMetadata> listFile(String parent, boolean showDeleted);
    // list dir all active files include version
    List<FileMetadata> listFile(String parent);
    // list dir all deleted files
    List<FileMetadata> listDeleted(String parent);

    Config getConfig();
    void housekeeping();

    public static class Config {

        public Config(int chunk_size, int delete_ttl, int insert_chunk_retry, int max_ver_size){
            this.CHUNK_SIZE= chunk_size;
            this.DELETE_TTL= delete_ttl;
            this.INSERT_CHUNK_RETRY= insert_chunk_retry;
            this.MAX_VER_SIZE= max_ver_size;
        }

        public final int CHUNK_SIZE;
        public final int DELETE_TTL;
        public final int INSERT_CHUNK_RETRY;
        public final int MAX_VER_SIZE;

    }

}
