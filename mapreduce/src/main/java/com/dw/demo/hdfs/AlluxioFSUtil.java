package com.dw.demo.hdfs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.*;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.List;

/**
 * Created by finup on 2017/11/6.
 */
public class AlluxioFSUtil {

    private final static Logger log = LoggerFactory.getLogger(AlluxioFSUtil.class);

    public static final int DEF_BLOCK_SIZE = 128;
    public static final int DEF_BUFFERED_SIZE = 4096;
    public static final String Encoding_UTF8 = "UTF-8";
    public static final String Encoding_GBK = "GBK";

    private static FileSystem fs;
    static {
        fs = FileSystem.Factory.get();
    }

    /**
     * alluxio 文件系统
     * @return
     * @throws Exception
     */
    private static synchronized FileSystem getFileSystem() throws Exception{
        if(null == fs) {
            fs = FileSystem.Factory.get();
        }
        return fs;
    }

    /**
     * alluxio uri
     * @param uri
     * @return
     * @throws Exception
     */
    public static AlluxioURI createURI(String uri) throws  Exception {
        Validate.notEmpty(uri, "alluxio.uri path is empty");
        return new AlluxioURI(uri);
    }


    /**
     * 定位策略
     * @return
     */
    public static CreateFileOptions createFileOptions(){
        CreateFileOptions fileopt = CreateFileOptions.defaults();
        fileopt.setRecursive(true);
        fileopt.setBlockSizeBytes(DEF_BLOCK_SIZE * Constants.MB);

        //轮询方式
        FileWriteLocationPolicy policy = new RoundRobinPolicy();
        fileopt.setLocationPolicy(policy);
        return fileopt;
    }


    /**
     * 创建目录
     * @param path
     * @throws Exception
     */
    public static boolean createDirectory(String path) throws Exception {
        FileSystem fs = getFileSystem();
        if(null != fs) {
            AlluxioURI uri = createURI(path);
            fs.createDirectory(uri);
            return true;
        }
        return false;
    }

    /**
     * 读文件
     * @param inputpath
     * @param encoding
     * @throws Exception
     */
    public static void readFile(String inputpath, String encoding) throws Exception {
        FileInStream is = null;
        BufferedInputStream bis = null;
        FileSystem fs = getFileSystem();
        try {
            if (null != fs) {
                AlluxioURI uri = createURI(inputpath);
                is = fs.openFile(uri);
                bis = new BufferedInputStream(is);

                for(String line : IOUtils.readLines(bis, encoding)) {
                    System.out.println(String.format("line=%s", line));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        } finally {
            if(null != bis){
                bis.close();
            }
            if(null != is){
                is.close();
            }
        }
    }


    /**
     * 读文件
     * @param inputpath
     * @param encoding
     * @throws Exception
     */
    public static void readFile4Recursion(String inputpath, String encoding) throws Exception {
        FileInStream is = null;
        BufferedInputStream bis = null;
        FileSystem fs = getFileSystem();

        try {
            if (null != fs) {
                AlluxioURI rootURI = createURI(inputpath);

                List<URIStatus> childsStatus = fs.listStatus(rootURI);

                StringBuilder sb = new StringBuilder();
                for(URIStatus childStatus : childsStatus){
                    String path = childStatus.getPath();
                    String udfpath = childStatus.getUfsPath();
                    int memperct = childStatus.getInMemoryPercentage();

                    boolean isFolder = childStatus.isFolder();
                    if(isFolder){

                        System.out.println(String.format("path=%s,udfpath=%s,memperct=%d", path, udfpath, memperct));

                        //递归迭代
                        readFile4Recursion(path, encoding);

                    }else{
                        AlluxioURI childURI = createURI(path);
                        is = fs.openFile(childURI);
                        bis = new BufferedInputStream(is);

                        for(String line : IOUtils.readLines(bis, encoding)) {
                            sb.append(line).append("===");
                            //System.out.println(String.format("line=%s", line));
                        }
                        System.out.println(String.format("path=%s,udfpath=%s,memperct=%d,info=%s", path, udfpath, memperct, sb.toString()));
                        sb.delete(0, sb.length());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        } finally {
            if(null != bis){
                bis.close();
            }
            if(null != is){
                is.close();
            }
        }
    }


    /**
     * 写文件
     * @param inputpath
     * @param outputpath
     * @throws Exception
     */
    public static void writFile(String inputpath, String outputpath) throws Exception {
        FileOutStream os = null;

        FileInputStream fis = null;
        BufferedInputStream bis = null;
        FileSystem fs = getFileSystem();
        try {
            if (null != fs) {
                //输入源
                fis = new FileInputStream(inputpath);
                bis = new BufferedInputStream(fis);

                AlluxioURI uri = createURI(outputpath);

                //输出源
                CreateFileOptions fileOptions = createFileOptions();
                os =  fs.createFile(uri, fileOptions);

                //buffer write
                org.apache.hadoop.io.IOUtils.copyBytes(bis, os, DEF_BUFFERED_SIZE);
                os.flush();
            }
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        } finally {
            if(null != bis){
                bis.close();
            }
            if(null != fis){
                fis.close();
            }
            if(null != os){
                os.close();
            }
        }
    }


    /**
     * 挂载数据源
     * @param alluxioPath alluxio上的命名空间路径
     * @throws Exception
     */
    public static boolean exist(String alluxioPath) throws Exception {
        FileSystem fs = getFileSystem();
        if (null != fs) {
            AlluxioURI mountURI = createURI(alluxioPath);

            return fs.exists(mountURI);
        }
        return false;
    }


    /**
     * 删除
     * @param alluxioPath
     * @return
     * @throws Exception
     */
    public static boolean delete(String alluxioPath,boolean recursive) throws Exception {
        FileSystem fs = getFileSystem();
        if (null != fs) {
            AlluxioURI mountURI = createURI(alluxioPath);
            if(fs.exists(mountURI)){
                fs.delete(mountURI, DeleteOptions.defaults().setRecursive(recursive));
                return true;
            }
        }
        return false;
    }


    /**
     * 挂载数据源
     * @param alluxioPath alluxio上的挂载点位置
     * @param ufsPath 用户数据源
     * @throws Exception
     */
    public static boolean mount(String alluxioPath, String ufsPath) throws Exception {
        FileSystem fs = getFileSystem();
        if (null != fs) {
            //alluxio挂载点
            AlluxioURI mountURI = createURI(alluxioPath);

            if(fs.exists(mountURI)){
                //输入源
                AlluxioURI inputURI = createURI(ufsPath);

                //挂载
                fs.mount(mountURI, inputURI);

                return true;
            }
        }
        return false;
    }


    /**
     * 取消挂载
     * @param alluxioPath alluxio上的挂载点位置
     * @throws Exception
     */
    public static boolean unmount(String alluxioPath) throws Exception {
        FileSystem fs = getFileSystem();
        if (null != fs) {
            //alluxio挂载点
            AlluxioURI alluxioURI = createURI(alluxioPath);

            if(fs.exists(alluxioURI)){
                //挂载
                fs.unmount(alluxioURI);

                return true;
            }
        }
        return false;
    }


    public static void main(String[] args) throws Exception {

        String path = "alluxio://finup:19998/data/20180715.parquet";
        String encoding = Encoding_UTF8;

        String allpath = "alluxio://finup:19998/";


        //1 读alluxio namespace
        readFile(path, encoding);

        //2 读alluxio namespace(含挂载 底层文件系统hdfs)
        //readFile4Recursion(allpath, encoding);

        String inpath = "/Users/finup/framework/data/demo.txt";
        String outpath = "alluxio://finup:19998/test/haha.txt";
        String outsource_path = "alluxio://finup:19998/out_t1/demo.txt";
        //writFile(inpath, outpath);

    }


}
