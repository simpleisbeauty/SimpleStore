package org.simple.repo.maven2;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface MavenStore {

    //get file input stream with size if possible
    InputStreamWSize getFile(String path) throws IOException;

    void putPath(String path, InputStream source) throws IOException;

    byte[] listPath(String path);

    static class InputStreamWSize{

        public InputStreamWSize(InputStream ins, long size){
            this.ins= ins;
            this.size= size;
        }

        public InputStream ins;
        public long size;
    }
}
