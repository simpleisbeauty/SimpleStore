package org.simple.repo.maven2;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.simple.store.DirMeta;
import org.simple.store.Store;
import org.simple.store.StoreInputStream;
import org.simple.store.StoreOutputStream;
import org.simple.store.StoreFactory;
import org.simple.store.FileMetadata;
import org.simple.store.cassandra.CassandraStoreFactory;

public class MavenCassandraStore implements MavenStore {

    @Override
    public InputStreamWSize getFile(String path) throws IOException {

        StoreInputStream sins = storeFactory.getStore(STORE_NAME).getInputStream(path);
        return new InputStreamWSize(sins, sins.size);
    }

    @Override
    public void putPath(String path, InputStream source) throws IOException {

        try (StoreOutputStream sos = storeFactory.getStore(STORE_NAME).getOutputStream(path)) {

            byte[] buf = new byte[8192];
            int len;
            while ((len = source.read(buf)) != -1) {
                sos.write(buf, 0, len);
                sos.EOF();
            }
        } catch (Exception e) {
            throw new IOException("put path:" + path, e);
        }
    }

    @Override
    public byte[] listPath(String path) {

        Store store = storeFactory.getStore(STORE_NAME);

        DirMeta dirMeta = store.listDir(path);
        StringBuilder sb = new StringBuilder("{\"path\":\"").append(path).append("\",\"last_modified\":").
                append(dirMeta.lastModified).append(",\"subdir\":[ ");
        dirMeta.subdir.forEach(n -> sb.append("\"").append(n).append("\","));
        sb.setCharAt(sb.length() - 1, ']'); // replace last , or empty with ]
        sb.append(", \"file\":[ ");
        List<FileMetadata> lst = store.listFile(path, false); // lst is not null
        lst.forEach(m -> sb.append(m).append(','));
        sb.setCharAt(sb.length() - 1, ']'); // replace last , with ]
        sb.append('}');
        return sb.toString().getBytes();
    }


    private final static String STORE_NAME;

    private final static StoreFactory storeFactory;

    static {
        STORE_NAME = System.getProperty("MAVEN_CASS_STORE", "maven2");
        storeFactory = new CassandraStoreFactory(9042,
                System.getProperty("cassandra.servers", "127.0.0.1"),
                "maven2.config");
    }
}
