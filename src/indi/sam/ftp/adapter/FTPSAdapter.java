package indi.sam.ftp.adapter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import indi.sam.ftp.util.FTPServiceUtil;
import indi.sam.ftp.util.Ftp;
import indi.sam.ftp.util.FtpThread;
import indi.sam.ftp.util.SendTool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.dc.eai.adapter.Adapter;
import com.dc.eai.config.AdapterConfig;
import com.dc.eai.data.Array;
import com.dc.eai.data.CompositeData;
import com.dc.eai.data.Field;

import com.dcfs.bob.gateway.exception.GatewayException;
import com.dcfs.bob.gateway.exception.GatewayExceptionConfig;

public class FTPSAdapter implements Adapter {
    enum HANDLER{download, upload};
    /**
     * log 输出变量
     */
    private static Log log = LogFactory.getLog(FTPSAdapter.class);

    private AdapterConfig config;
    private static final String encoding = "UTF-8";

    private ArrayList<FileEntity> fileList = null;
    private HashMap<String, String> fileMap = null;

    private static final String BODY_TAG = "body";
    private static final String DETAILS_TAG = "DETAILS";
    private static final String NAME_TAG = "name";
    private static final String FIELD_TAG = "field";
    private static final String ARRAY_TAG = "array";
    private static final String STRUCT_TAG = "struct";

    public static final String SrcFilePath_TAG = "SRCFILEPATH";
    public static final String SrcFileName_TAG = "SRCFILENAME";
    public static final String DestFilePath_TAG = "DESTFILEPATH";
    public static final String DestFileName_TAG = "DESTFILENAME";

    public static final String SeqNo_TAG = "SEQ_NO";
    public static final String Asyn_TAG = "ASYN";
    public static final String Status_TAG = "STATUS";

    private static final String SrcIP_TAG = "SRCIP";
    private static final String SrcPort_TAG = "SRCPORT";
    private static final String SrcUserId_TAG = "SRCUSER";
    private static final String SrcPwd_TAG = "SRCPWD";

    private static final String DestIP_TAG = "DESTIP";
    private static final String DestPort_TAG = "DESTPORT";
    private static final String DestUserId_TAG = "DESTUSER";
    private static final String DestPwd_TAG = "DESTPWD";
    private static final String SrcProtocol_TAG = "SRCPROTOCOL";
    private static final String DestProtocol_TAG = "DESTPROTOCOL";

    public static final String SYSHead_TAG = "SYS_HEAD";
    public static final String MsgType_TAG = "MESSAGE_TYPE";
    public static final String EAIHOME = "EAI_HOME";

    private FTPServiceUtil ftpUtil = new FTPServiceUtil();

    private static final String LOCALPATH = "shared/bj_conf/fileftp";
    private static final String WINDOWS_LOCALPATH = "d:\\temp\\DECD\\";
    private String defaultStoreFilePath = null;

//	public static void p(String s){
//		System.out.println(s);
//	}

    public Object doComm(Object request) {

        byte[] reqBytes = null;
        CompositeData reqCd = null;
        CompositeData sysHeadCd = null;
        SAXReader read = null;
        Document document = null;
        Element root = null;
        boolean connectFlag = false;
        boolean existFlag = false;
        boolean asyn = false;
        boolean needDown = false;
        boolean needUp = false;
        HashMap<String, Boolean> srcFilesStat = null;
        CompositeData respStruct = new CompositeData();

        this.fileList = new ArrayList<FileEntity>();
        this.fileMap = new HashMap<String, String>();

        String retCode = "0000";
        String retMsg = null;

        if(log.isDebugEnabled()){
            log.debug("FTP服务开始处理接出请求");
        }

        try{
            initFilePath();
            if(request == null){
                if(log.isErrorEnabled()){
                    log.error("[FileFTPAdapter]请求参数request为null。");
                }
            }else if(request instanceof CompositeData){
                if(log.isInfoEnabled()){
                    log.info("[FileFTPAdapter]请求参数request为CompositeData,需要先拆报文。");
                }
                reqCd = (CompositeData)request;
                sysHeadCd = reqCd.getStruct(SYSHead_TAG);
                plusMsgType(sysHeadCd);
                respStruct.addStruct(SYSHead_TAG, sysHeadCd);

                reqBytes = FTPServiceUtil.pack(reqCd);
            }else{
                if(log.isInfoEnabled()){
                    log.info("[FileFTPAdapter]请求参数request为byte数组。");
                }
                reqBytes = (byte[]) request;
            }
            if(reqBytes != null){


                read = new SAXReader();
                document = read.read(new ByteArrayInputStream(reqBytes));
                if(document != null){
                    if(log.isDebugEnabled()){
                        log.debug("FTP服务Adapter收到的报文内容为：[" + document.asXML() + "]");
                    }
                }else{
                    if(log.isErrorEnabled()){
                        log.error("[FileFTPAdapter]请求参数document为null。");
                    }
                    throw new GatewayException("002","请求空","请求报文为空",null);
                }

                //交给代理去处理
//				log.info("交给代理去处理.");
//				CompositeData respBody = proxySend(sysHeadCd, document);
//				return respBody;

                root = document.getRootElement();
                analysisPacket(root);

                if(this.fileList.size() > 0){
                    String asynTag = this.fileMap.get(Asyn_TAG);
                    if(asynTag != null && !"".equals(asynTag)){
                        asyn = asynTag.equals("true");
                    }else{
                        if(log.isInfoEnabled()){
                            log.info("异步标识为空，实施同步下载上传处理。");
                        }
                    }

                    Ftp srcFtp = getSrcFtp();
                    if(srcFtp != null){

                        if(log.isDebugEnabled()){
                            log.debug("需要执行到[" + srcFtp.getFtpAddress()
                                    + "][" + srcFtp.getPort() + "]的下载任务");
                        }
                        srcFtp = this.initFtp(srcFtp, HANDLER.download);
                    }

                    Ftp destFtp = getDestFtp();
                    if(destFtp != null){
//						p("需要执行到[" + destFtp.getFtpAddress()
//									+ "][" + destFtp.getPort() + "]的上传任务");
                        if(log.isDebugEnabled()){
                            log.debug("需要执行到[" + destFtp.getFtpAddress()
                                    + "][" + destFtp.getPort() + "]的上传任务");
                        }
                        destFtp = this.initFtp(destFtp, HANDLER.upload);
                    }
                    needUp = (destFtp != null);
                    needDown = (srcFtp != null);
                    srcFilesStat = transportFiles(srcFtp, destFtp, asyn, this.defaultStoreFilePath
                            , needDown, needUp );
                    if(srcFilesStat == null || srcFilesStat.isEmpty()){
                        if(log.isInfoEnabled()){
                            log.info("传输文件的状态表为空，判定登录失败。");
                        }
                        connectFlag = false;
                    }else{
                        connectFlag = true;
                        for(int i = 0; i < this.fileList.size(); i++){
                            FileEntity fileEntity = this.fileList.get(i);
                            String key = fileEntity.getSrcFilePath() + File.separator
                                    + fileEntity.getSrcFileName();

                            boolean status = srcFilesStat.get(key);
                            if(log.isInfoEnabled()){
                                log.info("文件[" + key + "]的状态表为[" + status + "]。");
                            }
                            existFlag = existFlag || status;
                            this.fileList.get(i).setStatus(status);
                        }
                    }


                    if(!connectFlag){
                        if(log.isErrorEnabled()){
                            log.error("[FileFTPAdapter]登录失败。");
                        }
                        retCode = "9999";
                        retMsg = retMsg + "登录失败.";
                    }
                    if(connectFlag && !existFlag){
                        if(log.isErrorEnabled()){
                            log.error("[FileFTPAdapter]文件不存在。");
                        }
                        retCode = "G301";
                        retMsg = retMsg + "文件不存在.";
                    }
                }else{
                    if(log.isInfoEnabled()){
                        log.info("明细数据为空，直接返回成功。");
                    }
                }


            }
        }catch(Exception e){


            respStruct = FTPServiceUtil.createFailMessage(
                    GatewayExceptionConfig.ADAPTER_COMM, "", "F", respStruct,
                    sysHeadCd.getField("SERVICE_CODE").strValue());
            e.printStackTrace();
        }
        respStruct = FTPServiceUtil.pack(respStruct, retCode, retMsg, this.fileList);
        if(log.isInfoEnabled()){
            log.info("FTPSAdapter处理完毕，返回报文为：[" + respStruct + "]");
        }
        return respStruct;
    }

    private void initFilePath() {
        if(System.getProperty("os.name").startsWith("Windows")){
            this.defaultStoreFilePath = WINDOWS_LOCALPATH;
        }else{
            String eaiHome = System.getProperty(EAIHOME);
            if (eaiHome == null || eaiHome.equals("")) {
                throw new GatewayException(GatewayExceptionConfig.MODULE_CONVERT,
                        GatewayExceptionConfig.UNPACK_CONFIG_ERROR,
                        "Can't find system property:[" + EAIHOME + "]",
                        null);
            }
            this.defaultStoreFilePath = eaiHome + File.separator + LOCALPATH;
        }

        if(log.isDebugEnabled()){
            log.debug("默认本地缓存地址为：[" + this.defaultStoreFilePath + "]");
        }
    }

    private CompositeData proxySend(CompositeData sysHeadCd, Document document) {
        log.debug("[Proxy]使用代理发送报文。");
        String respStr = new SendTool().sendPost(null, document.asXML());
        log.debug("[Proxy]代理返回的报文为：["+respStr+"]");
        CompositeData respBody = FTPServiceUtil.unpack(respStr.getBytes());
        log.debug("[Proxy]代理拆出来的CD报文为：["+respBody+"]");
        CompositeData retHead = respBody.getStruct(SYSHead_TAG);
        Array retArray = retHead.getArray("RET");
        sysHeadCd.addArray("RET", retArray);
        respBody.addStruct(SYSHead_TAG, sysHeadCd);
        log.debug("[Proxy]代理准备返回的CD报文为：["+respBody+"]");
        return respBody;
    }

    private void plusMsgType(CompositeData sysHeadCd) {
        Field field = sysHeadCd.getField(MsgType_TAG);
        if(field != null){
            String value = field.strValue();
            if(log.isDebugEnabled()){
                log.debug("请求报文的Message Type为：[" + value + "]");
            }if(value != null && !"".equals(value)){
                int type = Integer.valueOf(value);
                type = type + 10;
                Field newField = FTPServiceUtil.getStringField(String.valueOf(type));
                if(log.isDebugEnabled()){
                    log.debug("处理后的返回报文的Message Type为：[" + newField + "]");
                }
                sysHeadCd.addField(MsgType_TAG, newField);
            }

        }

    }

    private Ftp initFtp(Ftp ftp, HANDLER handler){
        if(log.isDebugEnabled()){
            log.debug("初始化Ftp中的文件路径时，fileList中的文件数量为：[" + this.fileList.size() + "]");
        }
        for(int i=0; i < this.fileList.size(); i++){
            FileEntity file = this.fileList.get(i);
            if(handler.equals(HANDLER.download)){
                ftp.addSrcFilePath(file.getSrcFilePath(), file.getSrcFileName());
            }else if(handler.equals(HANDLER.upload)){

                ftp.addDestFilePath(file.getDestFilePath(), file.getDestFileName());
            }
        }
        return ftp;
    }

    private HashMap<String, Boolean> transportFiles(Ftp srcftp, Ftp destftp, boolean asyn
            , String localRootDir, boolean needDown, boolean needUp){
        HashMap<String, Boolean> srcFilesStat = null;
        try {
            srcFilesStat = this.ftpUtil.startFtp(srcftp, destftp, localRootDir, asyn, needDown, needUp);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return srcFilesStat;
        }
        return srcFilesStat;
    }

    private void analysisPacket(Element root){
        Element data = null;
        if(root == null){
            if(log.isErrorEnabled()){
                log.error("[FileFTPAdapter]传入element根节点为空");
            }
            return;
        }else{
            if(log.isDebugEnabled()){
                log.debug("[FileFTPAdapter]待处理的报文内容：[" + root.asXML() + "]");
            }
            Element body = root.element(BODY_TAG);
            if(body == null){
                if(log.isErrorEnabled()){
                    log.error("[FileFTPAdapter]body节点为空");
                }
            }else{

                for(Iterator<Element> it = body.elementIterator();it.hasNext();){
                    data = it.next();
                    if(data.attributeValue(NAME_TAG).equals(DETAILS_TAG)){
                        if(log.isDebugEnabled()){
                            log.debug("将文件信息明细[" + data.asXML() + "]加入List中");
                        }
                        initList(data);

                    }else{
                        if(log.isDebugEnabled()){
                            log.debug("将地址信息[" + data.asXML() + "]加入Map中");
                        }
                        initMap(data);
                    }
                }
            }
        }
    }

    private void initList(Element data) {
        Element array = data.element(ARRAY_TAG);
        Element struct = null;
        Element childData = null;

        if(array != null){
            for(Iterator<Element> it = array.elementIterator();it.hasNext();){
                struct = it.next();
                if(log.isDebugEnabled()){
                    log.debug("本条明细信息为[" + struct.asXML() + "]");
                }
                HashMap<String, String> map = new HashMap<String, String>();

                for(Iterator<Element> it2 = struct.elementIterator();it2.hasNext();){
                    childData = it2.next();
                    String name = childData.attributeValue(NAME_TAG);
                    Element field = childData.element(FIELD_TAG);
                    String value = field.getText();
                    map.put(name, value);
                }
                if(!map.containsKey(SrcFileName_TAG) || map.get(SrcFileName_TAG) == null
                        || "".equals(map.get(SrcFileName_TAG))){
                    map.put(SrcFileName_TAG, FtpThread.FileList);
                }

                if(!map.containsKey(DestFilePath_TAG) || map.get(DestFilePath_TAG) == null
                        || "".equals(map.get(DestFilePath_TAG))){
                    map.put(DestFilePath_TAG, this.defaultStoreFilePath);
                }
                if(!map.containsKey(DestFileName_TAG) || map.get(DestFileName_TAG) == null
                        || "".equals(map.get(DestFileName_TAG))){
                    map.put(DestFileName_TAG, map.get(SrcFileName_TAG));
                }

                this.fileList.add(new FileEntity(map));
            }
        }
        if(log.isDebugEnabled()){
            log.debug("List初始化完成之后，fileList中的元素数量为:[" + this.fileList.size() + "]");
        }
        if(this.fileList.size() < 1){
            if(log.isInfoEnabled()){
                log.info("明细数据为空，直接返回成功。");
            }
        }

    }

    private void initMap(Element data) {
        String name = data.attributeValue(NAME_TAG);
        Element field = data.element(FIELD_TAG);
        if(field == null){
            log.error("field为空，element选择错误：[" + data.asXML() + "]");
        }else{
            String value = field.getText();
            this.fileMap.put(name, value);
        }


    }

    private Ftp getSrcFtp() {
        Ftp ftp = new Ftp(this.fileMap.get(SrcIP_TAG)
                , this.fileMap.get(SrcPort_TAG)
                , this.fileMap.get(SrcUserId_TAG)
                , this.fileMap.get(SrcPwd_TAG));
        ftp.setProtocol(this.fileMap.get(SrcProtocol_TAG));
        return ftp;
    }

    private Ftp getDestFtp() {
        Ftp ftp = null;
        if(this.fileMap.get(DestIP_TAG) != null
                && !"".equals(this.fileMap.get(DestIP_TAG))){
            ftp = new Ftp(this.fileMap.get(DestIP_TAG)
                    , this.fileMap.get(DestPort_TAG)
                    , this.fileMap.get(DestUserId_TAG)
                    , this.fileMap.get(DestPwd_TAG));
            ftp.setProtocol(this.fileMap.get(DestProtocol_TAG));
        }

        return ftp;
    }

    public AdapterConfig getConfig() {
        // TODO Auto-generated method stub
        return config;
    }
    /**
     * 设置通讯接出配置
     *
     * @param config
     */
    public void setConfig(AdapterConfig config) {
        this.config = config;
    }

    public static class FileEntity{
        private String SrcFilePath;
        private String SrcFileName;
        private String DestFilePath;
        private String DestFileName;
        private String SeqNo;
        private boolean status;

        public FileEntity(String SrcFilePath
                , String SrcFileName
                , String DestFilePath
                , String DestFileName){
            this.SrcFilePath = SrcFilePath;
            this.SrcFileName = SrcFileName;
            this.DestFilePath = DestFilePath;
            this.DestFileName = DestFileName;
        }
        public FileEntity(HashMap<String, String> map){//此处默认的是src的部分都会是有值的
            this.SrcFilePath = map.get(SrcFilePath_TAG);
            this.SrcFileName = map.get(SrcFileName_TAG);
            this.DestFilePath = map.get(DestFilePath_TAG);
            this.DestFileName = map.get(DestFileName_TAG);
            this.SeqNo = map.get(SeqNo_TAG);

            if(this.SrcFileName == null || "".equals(this.SrcFileName)){
                this.SrcFileName = FtpThread.FileList;
            }
        }

        public String getSrcFilePath() {
            return SrcFilePath;
        }

        public String getSrcFileName() {
            return SrcFileName;
        }

        public String getDestFilePath() {
            return DestFilePath;
        }

        public String getDestFileName() {
            return DestFileName;
        }
        public String getSeqNo() {
            return SeqNo;
        }
        public void setSeqNo(String seqNo) {
            SeqNo = seqNo;
        }
        public boolean getStatus() {
            return status;
        }
        public void setStatus(boolean status2) {
            this.status = status2;
        }
    }



}
