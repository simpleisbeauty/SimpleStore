package org.simple.store;

import org.simple.util.chunk.ChunkOutputStream;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class StoreOutputStream extends ChunkOutputStream {

    public StoreOutputStream(int chunkSize, ExecutorService exec, FileMetadata meta, Store store) {
        super(chunkSize, exec, c -> {
            Exception ex= null;
            for(int i=0; i< store.getConfig().INSERT_CHUNK_RETRY; i++) {
                try {
                    store.insertChunk(meta.id, c.sequence, c.data);
                    ex= null;
                    break;
                } catch (Exception e) { // must catch all Exception, because run in ExecutorService
                    ex= e;
                    try {
                        Thread.sleep(100);
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                }
            }
            if(ex != null){
                throw new RuntimeException(meta.toString()+ " seq: "+ c.sequence + "; split pos:" + c.pos, ex);
            }
        });
        this.meta = meta;
        this.store= store;
    }

    @Override
    public void close() throws IOException{
        if(isClosed())
            return;

        try {
            super.close();
            meta.chunks = getChunks();
            meta.size = this.length;
            store.insertMeta(meta);
            System.out.println("close: "+ meta.toString());
        }catch (Exception e){
            // clean up
            store.trash(meta.pn, meta.id);
            throw new IOException("close: "+ meta.toString(), e);
        }

    }

    public final FileMetadata meta;
    private Store store;
}
