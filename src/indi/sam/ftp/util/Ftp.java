package indi.sam.ftp.util;

import java.util.ArrayList;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class Ftp {

	private String ftpAddress; //ftp远程传输路径
	private int port; //ftp远程端口号
	private String username; //远程服务器用户名
	private String password; //远程服务器密码
	private String protocol = "F"; //S:sftp F:ftp
	private ArrayList<FTPFile> srcFilePaths;//from文件存放路径
	private ArrayList<FTPFile> destFilePaths;//dest文件存放路径

	public Ftp(String ftpAddress, String port, String username, String password){
		this.ftpAddress = ftpAddress;
		this.username = username;
		this.password = password;
		if(port == null){
			this.port = FTPClient.DEFAULT_PORT;
		}else{
			this.port = Integer.valueOf(port);
		}
		this.srcFilePaths = new ArrayList<FTPFile>();
		this.destFilePaths = new ArrayList<FTPFile>();
	}

	public ArrayList<FTPFile> getSrcFilePaths() {
		return srcFilePaths;
	}

	public ArrayList<FTPFile> getDestFilePaths() {
		return destFilePaths;
	}

	public void addSrcFilePath(String filePath, String fileName) {
		FTPFile file = new FTPFile();
		file.setLink(filePath);
		file.setName(fileName);
		file.setType(FTPFile.FILE_TYPE);
		this.srcFilePaths.add(file);
	}

	public void addDestFilePaths(ArrayList<FTPFile> filePaths){
		this.destFilePaths.addAll(filePaths);

	}

	public void addSrcFilePaths(ArrayList<FTPFile> filePaths){
		this.srcFilePaths.addAll(filePaths);

	}

	public void addDestFilePath(String filePath, String fileName) {
		FTPFile file = new FTPFile();
		file.setLink(filePath);
		file.setName(fileName);
		file.setType(FTPFile.FILE_TYPE);
		this.destFilePaths.add(file);
	}

	public String getFtpAddress() {
		return ftpAddress;
	}

	public int getPort() {
		return port;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String toString(){
		String result = "ftp地址：[" + ftpAddress
				+ "]端口：[" + port
				+ "]协议：[" + protocol
				+ "]用户名：[" + username
				+ "]源文件数量：[" + srcFilePaths.size()
				+ "]目标文件数量:[" + destFilePaths.size()
				+ "]";
		return result;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

}
