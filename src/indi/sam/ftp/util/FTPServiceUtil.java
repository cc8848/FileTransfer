package indi.sam.ftp.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import indi.sam.ftp.adapter.FTPSAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPFile;

import com.dc.eai.conv.InputPacket;
import com.dc.eai.conv.OutputPacket;
import com.dc.eai.conv.PackageConverter;
import com.dc.eai.data.Array;
import com.dc.eai.data.CompositeData;
import com.dc.eai.data.Field;
import com.dc.eai.data.FieldAttr;
import com.dc.eai.data.FieldType;

import com.dcfs.bob.gateway.exception.GatewayExceptionConfig;
import com.dcfs.bobjesb.conv.packconv.StandardESBPackageConverter;

public class FTPServiceUtil {
	private static Log log = LogFactory.getLog(FTPServiceUtil.class);

	public HashMap<String, Boolean> startFtp(Ftp srcFtp, Ftp destFtp,String localBaseDir, boolean asyn
			, boolean needDown, boolean needUp){
		FTPFile file = null;
		String destDir = null;
		HashMap<String, Boolean> srcFilesStat = null;
		if(!needDown){
			if(log.isInfoEnabled()){
				log.info("不需要下载文件");
			}
		}else{
			if(log.isDebugEnabled()){
				log.debug("下载的文件地址为：[" + srcFtp.getFtpAddress() + "]");
			}
			if(needUp){
				if(log.isDebugEnabled()){
					log.debug("上传的文件地址为：[" + destFtp.getFtpAddress() + "]");
				}
			}

			ArrayList<FTPFile> filelists = srcFtp.getSrcFilePaths();
			if(log.isDebugEnabled()){
				log.debug("开始Ftp下载处理时，filelists中的文件数量为：[" + filelists.size() + "]");
			}
			for(int i = 0; i < filelists.size(); i++ ){
				file = filelists.get(i);
				if(file.getLink() != null && !"".equals(file.getLink())){
					//空文件名就添加"FILELIST.txt"
					if(file.getName() == null || "".equals(file.getName())){
						file.setName(FtpThread.FileList);
					}
					destDir = localBaseDir + File.separator + file.getLink();
					destDir = FtpUtil.updateUrlByOS(destDir);
					srcFtp.addDestFilePath(destDir, file.getName());

					if(log.isDebugEnabled()){
						log.debug("文件下载到本地的路径为：[" + destDir + "]，文件名为：[" + file.getName() + "]");
					}

					if(needUp){
						destFtp.addSrcFilePath(destDir, file.getName());
						if(log.isDebugEnabled()){
							log.debug("文件上传的源路径为：[" + destDir + "]，文件名为：[" + file.getName() + "]");
						}
					}
				}
			}

			if(isFtp(srcFtp)){
				FtpUtil ftpUtil = new FtpUtil();
				srcFilesStat = ftpUtil.filesExist(srcFtp);
			}else{
				SFtpUtil sftpUtil = new SFtpUtil();
				srcFilesStat = sftpUtil.filesExist(srcFtp);
			}
			FtpThread ftpThread = new FtpThread(srcFtp, destFtp, localBaseDir, needDown, needUp);
			if(srcFilesStat == null || srcFilesStat.isEmpty()){
				if(log.isInfoEnabled()){
					log.info("检测文件是否存在的文件状态表为空，判定文件不存在，不再继续进行下载上传。");
				}
			}else{
				if(asyn){
					if(log.isDebugEnabled()){
						log.debug("文件按照异步进行处理");
					}
					Thread asynFtpThread = new Thread(ftpThread);
					asynFtpThread.start();
				}else{
					if(log.isDebugEnabled()){
						log.debug("文件按照同步进行处理");
					}
					ftpThread.run();
				}
			}

		}

		return srcFilesStat;
	}

	public static CompositeData pack(CompositeData respStruct, String retCode, String retMsg, ArrayList<FTPSAdapter.FileEntity> fileList){
		if(log.isDebugEnabled()){
			log.debug("[FTP服务工具]开始组返回报文");
		}


		CompositeData sysheadStruct = respStruct.getStruct(FTPSAdapter.SYSHead_TAG);

		Array retArray = new Array();

		CompositeData retStruct = new CompositeData();

		retStruct.addField("RET_CODE", getStringField(retCode));

		retStruct.addField("RET_MSG", getStringField(retMsg));

		retArray.addStruct(retStruct);

		sysheadStruct.addArray("RET", retArray);

		respStruct.addStruct(FTPSAdapter.SYSHead_TAG, sysheadStruct);

		Array detailsArray = new Array();

		for(int i=0; i < fileList.size(); i++){
			CompositeData detailStruct = new CompositeData();
			detailStruct.addField(FTPSAdapter.DestFilePath_TAG, getStringField(fileList.get(i).getDestFilePath()));
			detailStruct.addField(FTPSAdapter.DestFileName_TAG, getStringField(fileList.get(i).getDestFileName()));
			detailStruct.addField(FTPSAdapter.SeqNo_TAG, getStringField(fileList.get(i).getSeqNo()));
			detailStruct.addField(FTPSAdapter.Status_TAG, getStringField(String.valueOf(fileList.get(i).getStatus())));
			detailsArray.addStruct(detailStruct);
		}
//		respStruct.addArray("FTP_Details", detailsArray);
		respStruct.addArray("FTP_DETAILS", detailsArray);
//		OutputPacket outpack = new OutputPacket();

//		PackageConverter convert = new StandardESBPackageConverter();
//
//		convert.pack(outpack, respStruct, null);
//
//		byte[] cdxml = outpack.getBuff();

		return respStruct;
	}

	public static byte[] pack(CompositeData cdData){
		if(log.isDebugEnabled()){
			log.debug("[FTP服务工具]开始CompositeData报文转换为字节数组报文。");
		}
		OutputPacket outpack = new OutputPacket();

		PackageConverter convert = new StandardESBPackageConverter();

		convert.pack(outpack, cdData, null);

		byte[] result = outpack.getBuff();

		return result;
	}

	public static CompositeData unpack(byte[] bytes){
		CompositeData cdData = new CompositeData();
		InputPacket input = new InputPacket(bytes);
		PackageConverter convert = new StandardESBPackageConverter();
		convert.unpack(input, cdData, null);
		return cdData;
	}

	public static Field getStringField(String value) {

		if ((null == value) || (value.equals(""))) {
			FieldAttr fa = new FieldAttr(FieldType.FIELD_STRING, 0);
			Field field = new Field(fa);
			field.setValue("");
			return field;
		}
		FieldAttr fa = new FieldAttr(FieldType.FIELD_STRING, value.length());
		Field field = new Field(fa);
		field.setValue(value.trim());
		return field;
	}

	public static CompositeData createFailMessage(String errorCode, String msg,
												  String status, CompositeData data, String SERVICE_CODE) {
		// TODO Auto-generated method stub

		if (log.isDebugEnabled()) {
			log.debug("新结售汇动态 Create CUPS FailMessage, it's in data is:[" + data
					+ "]");
		}

		CompositeData failData = null;

		try {
			failData = GatewayExceptionConfig.getMBSDExceptionRetData(
					errorCode, msg, status, data, SERVICE_CODE);

			if (log.isDebugEnabled()) {
				log.debug("FailMessage is:[" + failData + "]");
			}
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("FTP Service动态 Create CUPS FailMessage catch[Exception]!", e);
			}
		}

		return failData;
	}

	public static boolean isFtp(Ftp f){
		String protocol = f.getProtocol();
		if(protocol == null || "".equals(protocol) || protocol.equalsIgnoreCase("F")){
			return true;
		}
		return false;
	}

}
