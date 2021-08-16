package org.simple.store.service;


import org.simple.store.*;

import org.simple.store.cassandra.CassandraStoreFactory;

import javax.servlet.AsyncContext;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;

import java.io.OutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;


@WebServlet( urlPatterns = {"/api/*"}, asyncSupported = true)
public class EndPointAPI extends HttpServlet {

    /**
     * Name of the directory where uploaded files will be saved, relative to
     * the web application directory.
     */


    protected void doPut(HttpServletRequest req,
                         HttpServletResponse res)  {
        String path = req.getPathInfo();

        AsyncContext ac = req.startAsync(req, res);
        ac.setTimeout(3000000); // may upload large file
        ForkJoinPool.commonPool().submit(() -> {

            ParentName pn= getStoreKey(path);
            Store store= storeFactory.getStore(pn.parent);
            try(StoreOutputStream cos= store.getOutputStream(pn.name)) {
                InputStream src= ac.getRequest().getInputStream();
                byte[] buf = new byte[8192];
                int len;
                while ((len = src.read(buf)) != -1) {
                    cos.write(buf, 0, len);
                }
                cos.EOF();
                res.getOutputStream().write(cos.meta.toString().getBytes());
            } catch (Exception e) {
                jsonError(e, res);
            }
            ac.complete();
        });
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) {

        String path = req.getPathInfo(); // store/parent/name
        AsyncContext ac = req.startAsync(req, res);
        ForkJoinPool.commonPool().submit(() ->{
            try {
                ParentName pn= getStoreKey(path);
                Store store= storeFactory.getStore(pn.parent);
                OutputStream os= res.getOutputStream();
                if(path.endsWith("/")){
                    res.setContentType("application/json");
                    DirMeta dirMeta = store.listDir(pn.name);
                    StringBuilder sb = new StringBuilder("{\"path\":\"").append(pn.parent).append("\",\"last_modified\":").
                            append(dirMeta.lastModified).append(",\"subdir\":[ ");
                    dirMeta.subdir.forEach(n -> sb.append("\"").append(n).append("\","));
                    sb.setCharAt(sb.length()-1, ']'); // replace last , or empty with ]
                    sb.append(", \"file\":[ ");
                    List<FileMetadata> lst = store.listFile(pn.name, "true".equals(req.getHeader("show_deleted"))); // lst is not null
                    lst.forEach(m -> sb.append(m).append(','));
                    sb.setCharAt(sb.length()-1, ']'); // replace last , with ]
                    sb.append('}');
                    os.write(sb.toString().getBytes());
                }
                else {
                    String t= req.getHeader("id");
                    try (StoreInputStream sins = (t== null? store.getInputStream(pn.name):
                            store.getInputStream(pn.name, UUID.fromString(t)))) {
                        byte[] buf = new byte[8192];
                        res.setContentLengthLong(sins.size);
                        int len;
                        while ((len = sins.read(buf)) != -1) {
                            os.write(buf, 0, len);
                        }
                    }
                }

            } catch (Exception e) {
                jsonError(e, res);
            }
            ac.complete();
        });
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse res)  {
        String path = req.getPathInfo();

        AsyncContext ac = req.startAsync(req, res);
        ForkJoinPool.commonPool().submit(() -> {

            try{
                ParentName pn= getStoreKey(path);
                Store store= storeFactory.getStore(pn.parent);
                String t= req.getHeader("act");
                if(t== null){
                    store.delete(pn.name);
                }
                else {
                    switch(Action.valueOf(t)){ // throw IllegalArgumentException
                        case delete_ver:
                            store.deleteVersion(FileMetadata.getParentAndName(pn.name), getVerId(req));
                            break;
                        case delete_recursive:
                            store.deleteRecursive(ParentName.isDir(pn.name));
                            break;
                        case restore:
                            store.restore(pn.name);
                            break;
                        case restore_ver:
                            store.restoreVersion(FileMetadata.getParentAndName(pn.name), getVerId(req));
                            break;
                        case restore_recursive:
                            store.restoreRecursive(ParentName.isDir(pn.name));
                            break;
                        case housekeeping:
                            store.housekeeping();
                            break;
                        case delete_store:
                            storeFactory.deleteStore(pn.parent);
                            break;
                        default:
                    }
                }

            } catch (Exception e) {
                jsonError(e, res);
            }
            ac.complete();
        });
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) {
        String path = req.getPathInfo();

        AsyncContext ac = req.startAsync(req, res);
        ForkJoinPool.commonPool().submit(() -> {

            try{
                Action act= Action.valueOf(req.getHeader("act")); // throw IllegalArgumentException

                ParentName pn= getStoreKey(path);
                Store store= storeFactory.getStore(pn.parent);
                switch(act){
                    case copy_ver:
                        store.copyVersion(pn.name, getVerId(req), req.getHeader("dest"));
                        break;
                    case copy:
                        store.copy(pn.name, req.getHeader("dest"));
                        break;
                    case move_ver:
                        store.moveVersion(pn.name, getVerId(req), req.getHeader("dest"));
                        break;
                    case move:
                        store.move(pn.name, req.getHeader("dest"));
                        break;
                    case copy_recursive:
                        store.copyRecursive(ParentName.isDir(pn.name), ParentName.isDir(req.getHeader("dest")));
                        break;
                    case move_recursive:
                        store.moveRecursive(ParentName.isDir(pn.name), ParentName.isDir(req.getHeader("dest")));
                        break;
                    case create_store:
                        storeFactory.createStore(pn.parent);
                        break;
                    default:
                }
            } catch (Exception e) {
                jsonError(e, res);
            }
            ac.complete();
        });
    }
    // /store/parent/name
    public static ParentName getStoreKey(String path){
        int idx= path.indexOf('/', 1);
        if(idx== -1){
            throw new IllegalArgumentException("no key after store name:"+ path);
        }
        return new ParentName(path.substring(1, idx), path.substring(idx));
    }


    private void jsonError(Exception e, HttpServletResponse res){
        res.setContentType("application/json");
        e.printStackTrace();
        try {
            res.getOutputStream().write(("{\"code\":500 , \"message\": \""+ e.getMessage()+"\"}").getBytes());
        } catch (IOException ex) {
        }

    }

    private UUID getVerId(HttpServletRequest req){
        return UUID.fromString(req.getHeader("id"));
    }

    private final static StoreFactory storeFactory;
    static {
        storeFactory = new CassandraStoreFactory(9042,
                        System.getProperty("cassandra.servers", "127.0.0.1"), "maven2.config");
    }
}
