package org.simple.repo.maven2;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

// alternative to Cassandra Maven Store
public class FileStore implements MavenStore{
    @Override
    public InputStreamWSize getFile(String path) throws IOException{
        String absPath= SAVE_DIR + path;
        try {
            File f= new File(absPath);
            return new InputStreamWSize(new FileInputStream(f), f.length());
        }catch (Exception e){
            throw new IOException("failed to get file:"+ path, e);
        }

    }

    @Override
    public void putPath(String path, InputStream source) throws IOException {
        String absPath = SAVE_DIR + path;
        try {
            File file = new File(absPath);
            File parent = file.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }

            FileOutputStream outStream = new FileOutputStream(file);

            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = source.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }

        }catch(Exception e){
            throw new IOException("put path:" + path, e);
        }
    }

    @Override
    public byte[] listPath(String path) {
        try {
            Path p = Paths.get(path);
            StringBuilder sb = new StringBuilder();
            Files.list(p).forEach(pth -> sb.append(pth.getFileName()).append("\n"));
            return sb.toString().getBytes();
        }catch(Exception e){
            return new byte[0];
        }
    }

    private static final String SAVE_DIR = "/tmp/m2";
}
