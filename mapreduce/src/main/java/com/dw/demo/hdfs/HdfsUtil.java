package com.dw.demo.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * hdfs工具类
 * @author root
 *
 */
public class HdfsUtil implements Serializable {
	
	private final static Logger log = LoggerFactory.getLogger(HdfsUtil.class);

	public static final int DEF_BUFFERED_SIZE = 4096;
	public static final String ROOT = "root";
	public static final String LINE_ENDING = "\n";

	private Configuration conf;
	private FileSystem fs;
	private String user;

	public HdfsUtil(Configuration conf){
		try {
			if(null != conf){
				this.conf = conf;
				this.fs = FileSystem.get(conf);
			}
		} catch (IOException e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}
	}

	public HdfsUtil(Configuration conf, String user){
		try {
			if(null != conf && !StringUtils.isEmpty(user)){
				this.conf = conf;
				URI uri = new URI(conf.get("fs.defaultFS"));
				this.fs = FileSystem.get(uri, conf, user);
				this.user = user;
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}
	}

//	//---HDFS客户端接口---------------------------------------------------
//
//	/**
//	 * 获取FileSystem
//	 * @return
//	 */

//
	public static FileSystem getFileSystem()
	{
		FileSystem fs = null;
		try {
			Configuration conf = new Configuration();
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fs;
	}
//
//	public static FileSystem getFileSystem(String user)
//	{
//		FileSystem fs = null;
//		Configuration conf = ConfigurationUtil.getHadoopConf();
//		try {
//			if(null != conf && !StringUtils.isEmpty(user)){
//				URI uri = new URI(conf.get("fs.defaultFS"));
//				fs = FileSystem.get(uri, conf, user);
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (URISyntaxException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		return fs;
//	}

	/**
	 * 关闭filesystem
	 */
	public boolean closeFileSystem(){
		boolean result = false;
		try {
			if(null != fs){
				fs.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 判断文件是否存在
	 * @return
	 */
	public boolean exitFile4HDFS(String file){
		boolean result = false;
		try {
			if(!StringUtils.isEmpty(file)){
				if(null != fs){
					Path hfile = new Path(file);
					result = fs.exists(hfile);
				}
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 创建目录
	 * @param dir
	 * @param overwrite
	 * @return
	 */
	public boolean createDict4HDFS(String dir,boolean overwrite){
		boolean result = false;
		try {
			if(!StringUtils.isEmpty(user) && !StringUtils.isEmpty(dir)){
				if(null != fs){
					Path hdir = new Path(dir);
					if(fs.exists(hdir)){
						if(overwrite){
							fs.delete(hdir, true);
						}
					}
					result = fs.mkdirs(hdir);
				}
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * 创建空文件
	 * @param file
	 * @return
	 */
	public boolean createBlankFile4HDFS(String file){
		boolean result = false;
		try {
			if(!StringUtils.isEmpty(user) && !StringUtils.isEmpty(file) ){
				if(null != fs){
					Path hfile = new Path(file);
					result = fs.createNewFile(hfile);
				}
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * 创建并写入文件
	 * @param is
	 * @return
	 */
	public boolean createAndWriteFile4HDFS(String file, InputStream is, boolean overwrite){
		boolean result = false;
		FSDataOutputStream fsdos = null;
		try {
			if(!StringUtils.isEmpty(user) && !StringUtils.isEmpty(file) && is.available() > 0){
				
				if(null != fs) {
					Path hfile = new Path(file);
					if (!fs.exists(hfile)) {
						fs.createNewFile(hfile);
					}
					fsdos = fs.append(hfile);


					IOUtils.copyBytes(is, fsdos, DEF_BUFFERED_SIZE);
					result = true;
				}
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			IOUtils.closeStream(is);
			IOUtils.closeStream(fsdos);
		}
		return result;
	}
	
	/**
	 * 删除文件
	 * @param file
	 * @return
	 */
	public boolean deleteFile4HDFS(String file){
		boolean result = false;
		try {
			if(!StringUtils.isEmpty(user) && !StringUtils.isEmpty(file) ){
				
				if(null != fs){
					Path hfile = new Path(file);
					result = fs.deleteOnExit(hfile);
				}
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	public boolean deleteFile4HDFS(String file,boolean delMode){
		boolean result = false;
		try {
			if(!StringUtils.isEmpty(file)){
				if(null != fs){
					Path hfile = new Path(file);
					if(fs.exists(hfile)){
						fs.delete(hfile,delMode);
					}
					result = true;
				}
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * 上传下载本地文件到hdfs
	 * @param localSrc
	 * @param hdfsDst
	 * @return
	 */
	public boolean uploadOrDownLocal4HDFS(String localSrc,String hdfsDst,boolean upload){
		boolean result = false;
		try {
			if(!StringUtils.isEmpty(user) && !StringUtils.isEmpty(localSrc)
					&& !StringUtils.isEmpty(hdfsDst)){

				if(null != fs){
					Path localSrc4Path = new Path(localSrc);
					Path hdfsDst4Path = new Path(hdfsDst);

					if(upload){
						fs.copyFromLocalFile(true,localSrc4Path, hdfsDst4Path);
					}else{
						fs.copyToLocalFile(false,localSrc4Path, hdfsDst4Path,true);
					}

					result = true;
				}

			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
	
	/**
	 * 查看hdfs目录结构
	 * @param path(支持递归调用???)
	 * @return
	 */
	public List<String> listStatus4HDFS(String path){
		List<String> result = new ArrayList<String>();
		try {
			if(!StringUtils.isEmpty(path)){
				if(null != this.fs){
					Path hpath = new Path(path);
					FileStatus[] fileStatusList = fs.listStatus(hpath);

					if(null != fileStatusList){
						for(FileStatus fss : fileStatusList){
							Path fPath = fss.getPath();
							//System.out.println(fPath.getName());
							result.add(fPath.toString());
						}
					}
				}
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	/**
	 * 读取数据流
	 * @param path
	 * @return
	 */
	public InputStream getInputStream4HDFS(String path){
		InputStream is = null;
		try {
			if(!StringUtils.isEmpty(path) ){
				
				if(null != fs){
					Path hpath = new Path(path);
					is = fs.open(hpath);
				}

			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return is;
	}
	
	
	/**
	 * 读取文件内容
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public List<String> readLines4HDFS(Path path) throws Exception {
        List<String> lines = new ArrayList<String>();
        FSDataInputStream fsis = null;
        BufferedOutputStream bos = null;
        try {
			if (null != path) {
				if(null != fs){
					fsis = fs.open(path);

					IOUtils.copyBytes(fsis, bos, DEF_BUFFERED_SIZE);
					org.apache.commons.io.IOUtils.writeLines(lines, LINE_ENDING, bos);
				}
//				br = new BufferedReader(new InputStreamReader(fs.open(path)));
//				String line;
//				while ((line = br.readLine()) != null) {
//					System.out.println(line);
//					lines.add(line);
//				}
			}
		} finally {
			try {
				if(null != fsis){
					IOUtils.closeStream(fsis);
				}
				if(null != bos){
					IOUtils.closeStream(bos);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return lines;
    }
	
	
	/**
	 * 上传目录(序列化)
	 * @param path
	 * @param local
	 * @return
	 */
	public boolean writeSequenceFile4Local(String path,String local){
		boolean result = false;
		SequenceFile.Writer writer = null;
		FileSystem fs = null;
		try {
			if(!StringUtils.isEmpty(path)
					&& !StringUtils.isEmpty(user)){
				
				if(null != fs){
					Path hpath = new Path(path);

					File localFile = new File(local);
					writer = SequenceFile.createWriter(fs, fs.getConf(), hpath, Text.class,
							Text.class, CompressionType.NONE);

					if(localFile.isDirectory()){
						File[] subFile = localFile.listFiles();
						for(File sub : subFile){
							Text key = new Text(localFile.getName());
							Text value = new Text(FileUtils.readFileToString(sub, "utf-8"));
							writer.append(key, value);
						}
					}else if(localFile.isFile()){
						Text key = new Text(localFile.getName());
						Text value = new Text(FileUtils.readFileToString(localFile, "utf-8"));
						writer.append(key, value);
					}

					result = true;
				}

			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			try{
				IOUtils.closeStream(writer);

			}catch(Exception e){
				e.printStackTrace();
			}
			
		}
		return result;
	}
	
	/**
	 * 下载序列化文件
	 * @param path
	 * @param local
	 * @return
	 */
	public void readSequenceFile4Local(String path, String local){
		SequenceFile.Reader reader = null;
		try {
			if(!StringUtils.isEmpty(path) && !StringUtils.isEmpty(local)){
				if(null != fs){
					Path hpath = new Path(path);
					reader = new SequenceFile.Reader(fs,hpath,fs.getConf());

					Text key = new Text();
					Text value = new Text();
					File dir = new File(local);
					if(!dir.exists()){
						dir.mkdirs();
					}

					while(reader.next(key)){
						File seqFile = new File(dir, key.toString());
						if(!seqFile.exists()){
							seqFile.createNewFile();
						}
						String valTxt = value.toString();
						InputStream is = org.apache.commons.io.IOUtils.toInputStream(valTxt, "utf-8");
						FileOutputStream fos = new FileOutputStream(seqFile);
						IOUtils.copyBytes(is, fos, 4096, true);
					}
				}

			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			try {
				if(null != reader){
					reader.close();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 写入二进制文件
	 * @param path
	 * @param local
	 * @throws Exception
	 */
	public void seq4Byte(String path, String local)  {
		try {
			if(!StringUtils.isEmpty(path) && !StringUtils.isEmpty(local)){
				if(null != fs){
					Path hpath = new Path(path);

					File localFile = new File(local);
					SequenceFile.Writer seq = SequenceFile.createWriter(fs, fs.getConf(), hpath,
							Text.class, BytesWritable.class,
							CompressionType.NONE);
					if(localFile.isDirectory()){
						for (File f : localFile.listFiles()) {
							byte[] imgData = FileUtils.readFileToByteArray(f);
							BytesWritable imgWritable = new BytesWritable(imgData);
							seq.append(new Text(f.getName()), imgWritable);
						}
					}else{
						byte[] imgData = FileUtils.readFileToByteArray(localFile);
						BytesWritable imgWritable = new BytesWritable(imgData);
						seq.append(new Text(localFile.getName()), imgWritable);
					}

					seq.close();
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 读取二进制文件
	 * @param path
	 * @param local
	 * @throws Exception
	 */
	public void seq4ReadByte(String path, String local) throws Exception {
		try {
			if(!StringUtils.isEmpty(path) && !StringUtils.isEmpty(local)){
				if(null != fs){
					Path hpath = new Path(path);

					SequenceFile.Reader reader = new SequenceFile.Reader(fs,hpath,fs.getConf());
					Text key = new Text();
					BytesWritable value = new BytesWritable();

					File dir = new File(local);
					File seqFile = dir;
					if(dir.isDirectory()){
						seqFile = new File(dir, key.toString());
						if(!seqFile.exists()){
							seqFile.createNewFile();
						}
					}
					while(reader.next(key, value)){
						System.out.println("Key=" + key);
						FileUtils.writeByteArrayToFile(seqFile, value.get());
						System.out.println("======================");
					}
					reader.close();
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public FileSystem getFs() {
		return fs;
	}

	public void setFs(FileSystem fs) {
		this.fs = fs;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String user = "root";

		//HdfsUtil hdfsUtil = new HdfsUtil(conf,user);

        HdfsUtil hdfsUtil = new HdfsUtil(conf);

		hdfsUtil.listStatus4HDFS("hdfs://node64:9000/");
	

		//String path = "hdfs://hdfsCluster/dl";
		//hdfsUtil.createBlankFile4HDFS(path);
		
		//deleteFile4HDFS(user, path);
		//03,054,p_054,31,70.22765781828639
		//createDict4HDFS(user, path, true);
		
		String local = "F:/sxt/data/spark-data-01.txt";
		boolean upload = true;

		//hdfsUtil.uploadOrDownLocal4HDFS(local,path,upload);

	}

}
