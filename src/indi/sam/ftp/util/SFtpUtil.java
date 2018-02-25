package indi.sam.ftp.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPFile;

import com.dcfs.bob.gateway.exception.GatewayException;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SFtpUtil{
	private static final Log log = LogFactory.getLog(SFtpUtil.class);

	//	private Ftp srcFtp;
//	private Ftp destFtp;
//	private String localBaseDir;
	private ChannelSftp sftp = null;
	private String rootDir = null;

//	private boolean needDown = false;
//	private boolean needUp = false;

//	public SFTPUtil(Ftp srcf, Ftp destf, String localBaseDirPara
//			, boolean needDown, boolean needUp){
//		this.srcFtp = srcf;
//		this.destFtp = destf;
//		this.localBaseDir = localBaseDirPara;
//		this.needDown = needDown;
//		this.needUp = needUp;
//	}
//	
//	public void run() {
//		if(this.localBaseDir == null){
//			if(log.isInfoEnabled()){
//				log.info("sftp本地默认地址为空。");
//			}
//		}
//		if(this.needDown){
//			if(this.srcFtp == null){
//				if(log.isErrorEnabled()){
//					log.error("sftp信息不完整无法执行下载任务");
//				}
//			}else{
//				try {
//					if(log.isInfoEnabled()){
//						log.info("开始执行到[" + this.srcFtp.getFtpAddress() + "][" + this.srcFtp.getUsername() + "]的下载，下载到本地的根目录为[" + this.localBaseDir + "]。");
//					}
//					excuteDownload(this.srcFtp, this.localBaseDir);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//
//		}
//		if(this.needUp){
//			if(this.destFtp == null){
//				if(log.isErrorEnabled()){
//					log.error("sftp信息不完整无法执行下载任务");
//				}
//			}else{
//				if(log.isInfoEnabled()){
//					log.info("开始执行到[" + this.destFtp.getFtpAddress() + "][" + this.destFtp.getUsername() + "]的上传。");
//				}
//				excuteUpload(this.destFtp);
//			}
//		}
//	}

	public HashMap<String, Boolean> filesExist(Ftp f) throws GatewayException{

		ArrayList<FTPFile> filelists= null;
		FTPFile file = null;
		boolean changedir = false;

		HashMap<String, Boolean> filesStateMap = new HashMap<String, Boolean>();

		if(log.isInfoEnabled()){
			log.info("开始验证[" + f.getFtpAddress() + "][" + f.getUsername() +"]的文件是否存在。");
		}
		try{
//			if(!waitForConnection(f)){
//				log.info("连接对方服务器失败。");
//				return filesStateMap;
//			}else{
//				log.info("连接对方服务器成功。");
//			}

			if(connectSFtp(f)){
				try{
					filelists = f.getSrcFilePaths();
					for(int i = 0; i < filelists.size(); i++ ){
						file = filelists.get(i);
//						if(rootDir != null){
//							if(!this.sftp.pwd().equals(rootDir)){
//								try{
//									this.sftp.cd(rootDir);
//								}catch(SftpException e ){
//									if(log.isErrorEnabled()){
//										log.error("切换到根目录[" + rootDir + "]时失败。");
//									}
//								}
//							}
//						}
						if(rootDir == null || "".equals(rootDir)){
							this.rootDir = this.sftp.getHome();
						}
						if(file.getLink() != null && !"".equals(file.getLink())){
							if(file.getName() != null && !"".equals(file.getName())){
								try{
									this.sftp.cd(this.rootDir + File.separator + file.getLink());
								}catch(SftpException e ){
									if(log.isInfoEnabled()){
										log.info("检测文件是否存在时，改变到工作目录[" + file.getLink() + "]失败。");
									}
									filesStateMap.put(file.getLink() + File.separator + file.getName(), false);
								}

								Vector<ChannelSftp.LsEntry> remoteFiles = this.sftp.ls(".");
								if(fileinList(remoteFiles, file.getName())){
									if(log.isInfoEnabled()){
										log.info("文件[" + file.getName() + "]存在于[" + file.getLink() + "]");
									}
									filesStateMap.put(file.getLink() + File.separator + file.getName(), true);
								}else{
									if(log.isInfoEnabled()){
										log.info("文件[" + file.getName() + "]并不存在于[" + file.getLink() + "]");
									}
									filesStateMap.put(file.getLink() + File.separator + file.getName(), false);
								}
							}
						}

					}
				}catch(Exception e){
					if(log.isErrorEnabled()){
						log.error("检查文件是否存在的过程中出现异常");
					}
					e.printStackTrace();
				}
			}else{
				if(log.isErrorEnabled()){
					log.error("[" + f.getFtpAddress() + "][" + f.getUsername() +"]连接失败");
				}
			}
		}catch(GatewayException e){
			if(log.isErrorEnabled()){
				log.error("登陆[" + f.getFtpAddress() + "][" + f.getPort() + "][" + f.getUsername() +"]的文件是否存在时出现异常。[" + e.getMessage() + "]");
			}
			throw e;
		}finally{
			closeSFtp();
		}
		return filesStateMap;
	}

	private boolean fileinList(Vector<ChannelSftp.LsEntry> remoteFiles, String name) {
		if(FtpThread.FileList.equals(name)){
			return true;
		}
		for(ChannelSftp.LsEntry ele : remoteFiles){
			if(ele.getFilename().equals(name)){
				return true;
			}
		}
		return false;
	}

	public void excuteDownload(Ftp f, String localBaseDir){

		ArrayList<FTPFile> filelists= null;
		FTPFile file = null;
		boolean changedir = false;
		try{
//			if(!waitForConnection(f)){
//				log.info("连接对方服务器失败。");
//				return ;
//			}else{
//				log.info("连接对方服务器成功。");
//			}

			if(connectSFtp(f)){
				try{
					filelists = f.getSrcFilePaths();
					for(int i = 0; i < filelists.size(); i++ ){
						file = filelists.get(i);
//						if(rootDir != null){
//							if(!this.sftp.pwd().equals(rootDir)){
//								if(log.isDebugEnabled()){
//									log.debug("先回到根目录[" + this.rootDir + "]");
//								}
//								try{
//									this.sftp.cd(rootDir);
//								}catch(SftpException e ){
//									if(log.isErrorEnabled()){
//										log.error("切换到根目录[" + rootDir + "]时失败。");
//									}
//								}
//							}
//						}
						if(rootDir == null || "".equals(rootDir)){
							rootDir = this.sftp.getHome();
						}

						if(file.getLink() != null && !"".equals(file.getLink())){
							if(file.getName() != null && !"".equals(file.getName())){
								try{
									this.sftp.cd(this.rootDir + File.separator + file.getLink());
									if(log.isDebugEnabled()){
										log.debug("进入到工作目录目录[" + this.sftp.pwd() + "]");
									}
									changedir = true;
								}catch(SftpException e ){
									if(log.isErrorEnabled()){
										log.error("进入到工作目录目录[" + file.getLink() + "]失败。");
									}
									changedir = false;
								}
								if(changedir){
									try{
										downloadFile(file, localBaseDir + File.separator + file.getLink());
									}catch(Exception e){
										if(log.isErrorEnabled()){
											log.error("[" + file.getLink() + file.getName() +"]下载失败");
										}
										e.printStackTrace();
										throw e;
									}
								}
							}else{
								if(log.isErrorEnabled()){
									log.error("文件路径：[" + file.getLink() + "],文件名：[" + file.getName() + "]，信息不完整，下载失败。");
								}
							}
						}

						if(changedir){}else{
							if(log.isErrorEnabled()){
								log.error("无法进入到目录[" + file.getLink() + "]中，下载失败，结束。");
							}
						}
					}
				}catch(Exception e){
					if(log.isErrorEnabled()){
						log.error("下载过程出现异常" + e);
					}
				}
			}else{
				if(log.isErrorEnabled()){
					log.error("[" + f.getFtpAddress() + "][" + f.getUsername() +"]连接失败");
				}
			}
		}catch(Exception e){
			if(log.isErrorEnabled()){
				log.error("登陆[" + f.getFtpAddress() + "][" + f.getUsername() +"]过程出现异常" + e);
			}
		}finally{
			closeSFtp();
		}

	}

	private void downloadFile(FTPFile file, String localBaseDir) {
		String tempDir = updateUrlByOS(localBaseDir);

		OutputStream output = null;
		boolean exist = false;
		if(log.isDebugEnabled()){
			log.debug("存放到本地的目录为[" + tempDir + "]");
		}
		if(file.isFile()){
			try{
				File dir = new File(tempDir);
				if(!dir.exists()){
					exist = dir.mkdirs();
					if(log.isDebugEnabled()){
						log.debug("目录[" + tempDir + "]不存在，创建该目录的结果为[" + exist + "]");
					}
				}else{
					exist = true;
				}
				if(exist){
					File localFile = new File(tempDir + file.getName());
					if(localFile.exists()){
						if(log.isDebugEnabled()){
							log.debug("目录[" + tempDir + "]下已经存在文件[" + file.getName() + "],需要将其覆盖。");
						}

					}

					output = new FileOutputStream(tempDir + file.getName());
					if(file.getName().equals(FtpThread.FileList)){
						Vector<ChannelSftp.LsEntry> remoteFiles = this.sftp.ls(".");
						StringBuffer sb = new StringBuffer();
						for(ChannelSftp.LsEntry ele : remoteFiles){
							sb.append(ele.getFilename() + "\n");
						}
						output.write(sb.toString().getBytes("UTF-8"));
					}else{
						this.sftp.get(file.getName(), output);
					}

					output.flush();
					if(log.isDebugEnabled()){
						log.debug("下载文件[" + file.getName() + "]到目录[" + tempDir + "]完成。");
					}
				}else{
					if(log.isDebugEnabled()){
						log.debug("目录[" + tempDir + "]不存在，创建该目录的结果为[" + exist + "]，不执行下载任务。");
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				if(output != null){
					try {
						output.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}else{
			if(log.isWarnEnabled()){
				log.warn("文件[" + file.getName() + "]不是文件属性[" + file.getType() + "]");
			}
		}
	}

	public void excuteUpload(Ftp f){
		ArrayList<FTPFile> srcFilelists= null;
		FTPFile srcFile = null;
		ArrayList<FTPFile> destFilelists= null;
		FTPFile destFile = null;
		boolean changedir = false;
		try{
//			if(!waitForConnection(f)){
//				log.info("连接对方服务器失败。");
//				return ;
//			}else{
//				log.info("连接对方服务器成功。");
//			}

			if(connectSFtp(f)){
				try{
					srcFilelists = f.getSrcFilePaths();
					destFilelists = f.getDestFilePaths();
					for(int i = 0; i < srcFilelists.size(); i++ ){
						srcFile = srcFilelists.get(i);
						destFile = destFilelists.get(i);
						if(srcFile.getLink() != null && !"".equals(srcFile.getLink())
								&& destFile.getLink() != null && !"".equals(destFile.getLink())){
							if(srcFile.getName() != null && !"".equals(srcFile.getName())
									&& destFile.getName() != null && !"".equals(destFile.getName())){
								if(log.isInfoEnabled()){
									log.info("执行文件的上传，源地址：[" + srcFile.getLink() + "]，文件名：[" + srcFile.getName()
											+ "]，目的地址[" + destFile.getLink() + "]，文件名：[" + destFile.getName() + "]");
								}
//								if(!this.sftp.pwd().equals(rootDir)){
//									if(log.isInfoEnabled()){
//										log.info("先回到根目录[" + this.rootDir + "]");
//									}
//									try{
//										this.sftp.cd(rootDir);
//										changedir = true;
//									}catch(SftpException e ){
//										if(log.isErrorEnabled()){
//											log.error("切换到根目录[" + rootDir + "]时失败。");
//										}
//										changedir = false;
//									}
//								}
								if(rootDir == null || "".equals(rootDir)){
									rootDir = this.sftp.getHome();
								}

								try{
									this.sftp.cd(this.rootDir + File.separator + destFile.getLink());
									if(log.isInfoEnabled()){
										log.info("进入到工作目录目录[" + this.sftp.pwd() + "]");
									}
									changedir = true;
								}catch(SftpException e ){
									if(log.isErrorEnabled()){
										log.error("进入到工作目录目录[" + destFile.getLink() + "]失败。");
									}
									changedir = false;
								}

								if(!changedir){
									try{
										this.sftp.mkdir(this.rootDir + File.separator + destFile.getLink());
										changedir = true;
									}catch(SftpException e ){
										if(log.isErrorEnabled()){
											log.error("无法找到[" + destFile.getLink() + "]目录,也无法创建该目录。");
										}
										changedir = false;
									}
									if(changedir){
										try{
											this.sftp.cd(this.rootDir + File.separator + destFile.getLink());
											changedir = true;
											if(log.isInfoEnabled()){
												log.info("创建一级目录后，当前工作目录为：[" + this.sftp.pwd() + "]");
											}
										}catch(SftpException e){
											if(log.isErrorEnabled()){
												log.error("进入到工作目录目录[" + destFile.getLink() + "]失败。");
											}
											changedir = false;
										}
									}
								}
								if(changedir){
									try{
										uploadFile(srcFile, destFile);
									}catch(Exception e){
										if(log.isErrorEnabled()){
											log.error("[" + srcFile.getLink() + File.separator + srcFile.getName() +"]上传失败" + e);
										}
									}
								}


							}
						}
					}
				}catch(Exception e){
					if(log.isErrorEnabled()){
						log.error("上传过程出现异常" + e);
					}
				}
			}else{
				if(log.isErrorEnabled()){
					log.error("[" + f.getFtpAddress() + "][" + f.getUsername() +"]连接失败");
				}
			}
		}catch(Exception e){
			if(log.isErrorEnabled()){
				log.error("登陆[" + f.getFtpAddress() + "][" + f.getUsername() +"]过程出现异常" + e);
			}
		}finally{
			closeSFtp();
		}

	}

	private void uploadFile(FTPFile srcFile, FTPFile destFile) {

		FileInputStream input = null;
		File file = new File(srcFile.getLink() + File.separator + srcFile.getName());
		try{
			if(file.isFile()){
				if(log.isInfoEnabled()){
					log.info("源文件地址为：[" + srcFile.getLink() + File.separator + srcFile.getName() + "]");
				}
				input = new FileInputStream(srcFile.getLink() + File.separator + srcFile.getName());
				this.sftp.put(input, destFile.getName());
			}else{
				if(log.isErrorEnabled()){
					log.error("[" + srcFile.getLink() + File.separator + srcFile.getName() +"]不是文件。");
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(input != null){
				try {
					input.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private boolean connectSFtp(Ftp f) throws GatewayException{
		boolean success = false;
		if(log.isDebugEnabled()){
			log.debug("开始链接登录sftp：[" + f.getFtpAddress() + "][" + f.getUsername() + "]端口[" + f.getPort() + "]");
		}
		try{
			JSch jsch = new JSch();
			jsch.getSession(f.getUsername(), f.getFtpAddress(), f.getPort());
			Session sshSession = jsch.getSession(f.getUsername(), f.getFtpAddress(), f.getPort());
			if(log.isInfoEnabled()){
				log.info("Session created!");
			}
			sshSession.setPassword(f.getPassword());
			Properties sshConfig = new Properties();
			sshConfig.put("StrictHostKeyChecking", "no");
			sshSession.setConfig(sshConfig);
			sshSession.connect();
			if(log.isInfoEnabled()){
				log.info("Session connected!");
				log.info("Opening Channel!");
			}
			Channel channel = sshSession.openChannel("sftp");
			channel.connect();
			this.sftp = (ChannelSftp) channel;
			if(log.isInfoEnabled()){
				log.info("Connected to:[" + f.getFtpAddress() + "],[" + f.getPort() + "]");
			}

			rootDir = this.sftp.pwd();
			success = true;
		}catch(Exception e){
			success = false;
			if(e instanceof GatewayException){
				throw (GatewayException)e;
			}else{
				log.error("登录sftp：[" + f.getFtpAddress() + "][" + f.getUsername() + "]端口[" + f.getPort() + "]出现错误");
			}

		}
		if(log.isDebugEnabled()){
			log.debug("连接状态为：[" + success + "]");
		}
		return success;
	}

	private void closeSFtp(){
		if(log.isDebugEnabled()){
			log.debug("开始关闭到sftp的连接");
		}
		if(this.sftp != null && this.sftp.isConnected()){
			this.sftp.disconnect();
		}else if(this.sftp.isClosed()){
			if(log.isInfoEnabled()){
				log.info("sftp连接已经关闭了。");
			}
		}
	}

	public static String updateUrlByOS(String localBaseDir) {
		String tempDir = localBaseDir.replace("/", File.separator);
		tempDir = tempDir.replace("\\", File.separator) + File.separator;
		if(log.isDebugEnabled()){
			log.debug("根据操作系统整理过后的文件地址为：[" + tempDir + "]");
		}
		return tempDir;
	}

//	private boolean waitForConnection(Ftp f) {
//		FtpDetect detect = new FtpDetect();
//		int i = 0 ;
//		while(!detect.detectSFtp(f)){
//			log.info("尝试连接sftp···");
//			try {
//				Thread.sleep(2000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			if(i++ > 20){
//				return false;
//			}
//		}
//		return true;
//	}


}
