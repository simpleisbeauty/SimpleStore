package org.simple.store;

import com.datastax.driver.core.utils.UUIDs;

import java.util.UUID;


public class FileMetadata {

    public FileMetadata(String store, String key, long size, int chunks) {
        this(store, FileMetadata.getParentAndName(key), size, chunks, UUIDs.timeBased(), true, 0);
    }

    public FileMetadata(String store, ParentName pn, long size, int chunks,
                        UUID id, boolean active, long createTime) {

        this.store = store;
        this.pn = pn;
        this.id = id;
        this.size = size;
        this.chunks = chunks;
        this.active = active;
        this.createTime= createTime;
    }

    public final ParentName pn;
    public final String store;
    public UUID id;
    public long size;
    public int chunks;
    public final boolean active;
    public final long createTime;



    public static ParentName getParentAndName(String key){
        int l= key.length();
        if(key.charAt(l-1)== '/'){ // end is folder
            l= l-2;
        }
        int idx= key.lastIndexOf('/', l);
        if(idx== -1){
            throw new IllegalArgumentException(key + " has no /");
        }
        String parent= key.substring(0, idx+ 1);
        if (parent.length()== 0){
            parent= "/";
        }

        String name= key.substring(idx+1);
        return new ParentName(parent, name);
    }

    @Override
    public String toString() {
        return new StringBuilder("{\"store\":\"").append(store).append("\",").append(pn).append(", \"id\":\"").append(id).
                append("\", \"size\":").append(size).append(", \"chunks\":").append(chunks).
                append(", \"created\":").append(createTime).append(", \"active\":").append(active).append('}')
                .toString();
    }

}


