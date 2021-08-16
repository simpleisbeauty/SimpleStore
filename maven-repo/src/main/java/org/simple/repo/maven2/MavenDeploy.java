package org.simple.repo.maven2;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStream;


import java.util.concurrent.ForkJoinPool;


@WebServlet(name = "/MavenDeployServlet", urlPatterns = {"/repo/*"}, asyncSupported = true)
public class MavenDeploy extends HttpServlet {

    /**
     * Name of the directory where uploaded files will be saved, relative to
     * the web application directory.
     */


    /**
     * handles file upload
     */
    protected void doPut(HttpServletRequest req,
                         HttpServletResponse res) throws IOException {
        String path = req.getPathInfo();

        AsyncContext ac = req.startAsync(req, res);
        ac.setTimeout(3000000);
        ForkJoinPool.commonPool().submit(() -> {

            ServletRequest sr = ac.getRequest();
            try {
                store.putPath(path, sr.getInputStream());
                System.out.println(path+ " - put succeeded");
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    res.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                } catch (IOException ex) {
                }
            }
            ac.complete();
        });
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {

        String path = req.getPathInfo();
        AsyncContext ac = req.startAsync(req, res);
        ForkJoinPool.commonPool().submit(() -> {
            try {
                OutputStream os= res.getOutputStream();
                if(path.endsWith("/")) {
                    res.setContentType("application/json");
                    os.write(store.listPath(path));
                }
                else{
                    MavenStore.InputStreamWSize insize= store.getFile(path);
                    byte[] buf = new byte[8192];
                    res.setContentLengthLong(insize.size);
                    int len;
                    while ((len = insize.ins.read(buf)) != -1) {
                        os.write(buf, 0, len);
                    }
                }
            } catch (Exception e) {
                try {
                    // maven deploy-file must see 404 status for any get ERROR to upload maven-metadata and md5, sha1
                    res.sendError(HttpServletResponse.SC_NOT_FOUND);
                } catch (IOException ex) {
                }
            }
            ac.complete();
        });
    }



    private MavenStore store = new MavenCassandraStore();
}
