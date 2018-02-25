package indi.sam.ftp.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class FtpThread implements Runnable {
	private static final Log log = LogFactory.getLog(FtpThread.class);

	public static final String FileList = "FILELIST.txt";

	private Ftp srcFtp;
	private Ftp destFtp;
	private String localBaseDir;

	private boolean needDown = false;
	private boolean needUp = false;

	public FtpThread(Ftp srcf, Ftp destf, String localBaseDirPara
			, boolean needDown, boolean needUp){
		this.srcFtp = srcf;
		this.destFtp = destf;
		this.localBaseDir = localBaseDirPara;
		this.needDown = needDown;
		this.needUp = needUp;
	}

	public void run() {
		if(this.localBaseDir == null){
			if(log.isInfoEnabled()){
				log.info("ftp本地默认地址为空。");
			}
		}
		if(this.needDown){
			if(this.srcFtp == null){
				if(log.isErrorEnabled()){
					log.error("ftp信息不完整无法执行下载任务");
				}
			}else{
				try {
					if(log.isInfoEnabled()){
						log.info("开始执行到[" + this.srcFtp.getFtpAddress() + "][" + this.srcFtp.getUsername() + "]的下载，下载到本地的根目录为[" + this.localBaseDir + "]。");
					}
					if(isFtp(this.srcFtp)){
						FtpUtil ftputil = new FtpUtil();
						ftputil.excuteDownload(this.srcFtp, this.localBaseDir);
					}else{
						SFtpUtil sftputil = new SFtpUtil();
						sftputil.excuteDownload(this.srcFtp, this.localBaseDir);
					}

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		if(this.needUp){
			if(this.destFtp == null){
				if(log.isErrorEnabled()){
					log.error("ftp信息不完整无法执行下载任务");
				}
			}else{
				if(log.isInfoEnabled()){
					log.info("开始执行到[" + this.destFtp.getFtpAddress() + "][" + this.destFtp.getUsername() + "]的上传。");
				}
				if(isFtp(this.destFtp)){
					FtpUtil ftputil = new FtpUtil();
					ftputil.excuteUpload(this.destFtp);
				}else{
					SFtpUtil sftputil = new SFtpUtil();
					sftputil.excuteUpload(this.destFtp);
				}

			}
		}

	}

	public static boolean isFtp(Ftp f){
		String protocol = f.getProtocol();
		if(protocol == null || "".equals(protocol) || protocol.equalsIgnoreCase("F")){
			return true;
		}
		return false;
	}

}
