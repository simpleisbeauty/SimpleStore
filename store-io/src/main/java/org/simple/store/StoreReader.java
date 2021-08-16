package org.simple.store;

import org.simple.util.chunk.ChunkReader;
import org.simple.util.chunk.ChunkStore;

import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

public class StoreReader extends ChunkReader {

    public StoreReader(ExecutorService exec, FileMetadata meta, OutputStream dest, Store store) {
        super(exec, meta.chunks, seq -> {
            ChunkStore.Chunk chunk= new ChunkStore.Chunk(seq.intValue(), 0);;
            try {
                chunk.data= store.readChunk(meta.id, seq);
                chunk.pos= chunk.data.length;
                //compressed data
            }
            catch(Exception ioe){
                chunk.status=-1;
                ioe.printStackTrace();
            }
            return chunk;
        }, dest);
        this.meta= meta;
    }

    public final FileMetadata meta;
}
