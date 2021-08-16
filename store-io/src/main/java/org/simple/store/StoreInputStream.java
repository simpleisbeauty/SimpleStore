package org.simple.store;

import org.simple.util.chunk.ChunkInputStream;
import static org.simple.util.chunk.ChunkStore.Chunk;
import java.util.concurrent.ExecutorService;

public class StoreInputStream extends ChunkInputStream {

    public StoreInputStream(ExecutorService exec, FileMetadata meta, Store store) {

        super(exec, meta.size, meta.chunks, seq -> {
            Chunk chunk= new Chunk(seq.intValue(), 0);;

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
        });
    }

}
