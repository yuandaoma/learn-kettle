package com.yuandaoma.code.kettleDemo;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputMeta;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputMeta;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.Schemes;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.file.BaseFileField;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.runtime.test.action.impl.RuntimeTestActionServiceImpl;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class HadoopFileInputOutputTransTest {


    public static final String HDFS_PROTO = "pentaho-hdfs://";
    public static final String HDFS_FILE = "2007-10.csv";
//    public static final String HDFS_FILE = "a.csv";
    public static final String HDFS_CLUSTE_NAME = "node1";
    public static final String HDFS_HOST ="node1";
    public static final String HDFS_PORT = "8020";
    public static final String HDFS_USERNAME = "";
    public static final String HDFS_PASSWORD = "";
    public static final String HDFS_JOB_TRACKER_HOST = "node1";
    public static final String HDFS_JOB_TRACKER_PORT = "8032";
    public static final String ZOOKEEPER_HOST = "node1";
    public static final String ZOOKEEPER_PORT = "8032";



    public static void main(String[] args) throws Exception {
        try {
            KettleEnvironment.init();
            NamedClusterManager clusterManager = new NamedClusterManager();
            NamedCluster cluster = new NamedClusterImpl();
            cluster.setStorageScheme(Schemes.HDFS_SCHEME);
            cluster.setHdfsHost(HDFS_HOST);
            cluster.setHdfsPort(HDFS_PORT);
            cluster.setName(HDFS_CLUSTE_NAME);
            cluster.setHdfsUsername(HDFS_USERNAME);
            cluster.setHdfsPassword(HDFS_PASSWORD);
            cluster.setJobTrackerHost(HDFS_JOB_TRACKER_HOST);
            cluster.setJobTrackerPort(HDFS_JOB_TRACKER_PORT);
            cluster.setZooKeeperHost(ZOOKEEPER_HOST);
            cluster.setZooKeeperPort(ZOOKEEPER_PORT);
            clusterManager.setClusterTemplate(cluster);

            HadoopFileInputMeta hadoopFileInputMeta = new HadoopFileInputMeta(clusterManager,
                    new RuntimeTestActionServiceImpl(null, null),
                    new RuntimeTesterImpl(null, null, "hadoopInput"));
            hadoopFileInputMeta.allocateFiles(1);
            hadoopFileInputMeta.setFileName(new String[]{"pentaho-hdfs://" + HDFS_CLUSTE_NAME + ":" + HDFS_PORT + "/" + HDFS_FILE});
            BaseFileField[] fileFields = parseFileFields();
            hadoopFileInputMeta.inputFields = fileFields;
            hadoopFileInputMeta.content.fileType = "CSV";
            hadoopFileInputMeta.content.enclosure = "\"";
            hadoopFileInputMeta.content.separator = ",";
            hadoopFileInputMeta.content.header =  true;
            hadoopFileInputMeta.content.nrHeaderLines = 1;
            hadoopFileInputMeta.content.footer = false;
            hadoopFileInputMeta.content.nrFooterLines = 1;
            hadoopFileInputMeta.content.lineWrapped = false;
            hadoopFileInputMeta.content.nrWraps = 1;
            hadoopFileInputMeta.content.layoutPaged = false;
            hadoopFileInputMeta.content.nrLinesPerPage = 80;
            hadoopFileInputMeta.content.nrLinesDocHeader = 0;
            hadoopFileInputMeta.content.noEmptyLines = false;
            hadoopFileInputMeta.content.rowNumberByFile = false;
            hadoopFileInputMeta.content.fileFormat = "Unix";
            hadoopFileInputMeta.content.encoding = "UTF-8";
            hadoopFileInputMeta.content.fileCompression= "None";
            // 跳过错误行
            hadoopFileInputMeta.setErrorLineSkipped(true);

            HadoopFileOutputMeta hadoopFileOutputMeta = new HadoopFileOutputMeta(clusterManager,
                    new RuntimeTestActionServiceImpl(null, null),
                    new RuntimeTesterImpl(null, null, "hadoopOutput"));

            String date = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
            System.out.println("date = " + date);
            hadoopFileOutputMeta.setSourceConfigurationName("cluster");
            hadoopFileOutputMeta.setOutputFields(new TextFileField[]{});
            hadoopFileOutputMeta.setFileName("pentaho-hdfs://node1:8020/" + date + "/cc");
            hadoopFileOutputMeta.setExtension("txt");
            // 分割符
            hadoopFileOutputMeta.setSeparator(";");
            // 封闭符
            hadoopFileOutputMeta.setEnclosure("\"");
            // 编码
            hadoopFileOutputMeta.setEncoding("UTF-8");
            // 是否强制在字段周围加封闭符号
            hadoopFileOutputMeta.setEnclosureForced(false);
            // 是否允许头部
            hadoopFileOutputMeta.setHeaderEnabled(true);
            hadoopFileOutputMeta.setDateInFilename(true);
            hadoopFileOutputMeta.setTimeInFilename(true);
            // 是否允许尾部
            hadoopFileOutputMeta.setFooterEnabled(false);
            // 文件压缩格式：GZip,Hadoop-snappy,None,Snappy,Zip
            hadoopFileOutputMeta.setFileCompression("None");
            // 文件格式 CR+LR terminated(Windows,DOS) -> DOS
            // LF terminated(Unix)
            // CR terminated
            // No new-line terminator
            hadoopFileOutputMeta.setFileFormat("DOS");
            // 添加文件结束行
            hadoopFileOutputMeta.setEndedLine("");
            // 从字段中获取文件名
            hadoopFileOutputMeta.setFileNameInField(false);
            // 创建父目录
            hadoopFileOutputMeta.setCreateParentFolder(true);

            TransMeta transMeta = new TransMeta();
            StepMeta fromStep = new StepMeta("hadoopFileInput", hadoopFileInputMeta);
            fromStep.setDistributes(true);
            fromStep.setCopies(1);
            StepMeta toStep = new StepMeta("hadoopFileOutput", hadoopFileOutputMeta);
            toStep.setDistributes(true);
            toStep.setCopies(1);
            transMeta.addStep(fromStep);
            transMeta.addStep(toStep);
            TransHopMeta transHopMeta = new TransHopMeta();
            transHopMeta.setFromStep(fromStep);
            transHopMeta.setToStep(toStep);
            transHopMeta.setEnabled(true);
            transMeta.addTransHop(transHopMeta);

            String fileName = "T:\\test-c.ktr";
            String xml = transMeta.getXML();
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(fileName)));
            dos.write(xml.getBytes("UTF-8"));
            dos.close();
            System.out.println("保存完成");

            Trans trans = new Trans(transMeta);
            trans.setLogLevel(LogLevel.ROWLEVEL);
            trans.execute(null);
            trans.waitUntilFinished();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static BaseFileField[] parseFileFields() {
        BaseFileField[] result = new BaseFileField[8];
        BaseFileField fileField = new BaseFileField();
        fileField.setName("aa");
        fileField.setType("String");
        fileField.setCurrencySymbol("￥");
        fileField.setDecimalSymbol(".");
        fileField.setGroupSymbol(",");
        fileField.setNullString("-");
        fileField.setPosition(-1);
        fileField.setLength(5);
        fileField.setPrecision(-1);
        fileField.setTrimType("none");
        fileField.setRepeated(false);
        result[0] = fileField;

        fileField = new BaseFileField();
        fileField.setName("bb");
        fileField.setType("Integer");
        fileField.setFormat("#");
        fileField.setCurrencySymbol("￥");
        fileField.setDecimalSymbol(".");
        fileField.setGroupSymbol(",");
        fileField.setNullString("-");
        fileField.setPosition(-1);
        fileField.setLength(15);
        fileField.setPrecision(0);
        fileField.setTrimType("none");
        fileField.setRepeated(false);
        result[1] = fileField;


        fileField = new BaseFileField();
        fileField.setName("cc");
        fileField.setType("Integer");
        fileField.setFormat("#");
        fileField.setCurrencySymbol("￥");
        fileField.setDecimalSymbol(".");
        fileField.setGroupSymbol(",");
        fileField.setNullString("-");
        fileField.setPosition(-1);
        fileField.setLength(15);
        fileField.setPrecision(0);
        fileField.setTrimType("none");
        fileField.setRepeated(false);
        result[2] = fileField;


        fileField = new BaseFileField();
        fileField.setName("dd");
        fileField.setType("Integer");
        fileField.setFormat("#");
        fileField.setCurrencySymbol("￥");
        fileField.setDecimalSymbol(".");
        fileField.setGroupSymbol(",");
        fileField.setNullString("-");
        fileField.setPosition(-1);
        fileField.setLength(15);
        fileField.setPrecision(0);
        fileField.setTrimType("none");
        fileField.setRepeated(false);
        result[3] = fileField;

        fileField = new BaseFileField();
        fileField.setName("ee");
        fileField.setType("Integer");
        fileField.setFormat("#");
        fileField.setCurrencySymbol("￥");
        fileField.setDecimalSymbol(".");
        fileField.setGroupSymbol(",");
        fileField.setNullString("-");
        fileField.setPosition(-1);
        fileField.setLength(15);
        fileField.setPrecision(0);
        fileField.setTrimType("none");
        fileField.setRepeated(false);
        result[4] = fileField;

        fileField = new BaseFileField();
        fileField.setName("ff");
        fileField.setType("String");
        fileField.setCurrencySymbol("￥");
        fileField.setDecimalSymbol(".");
        fileField.setGroupSymbol(",");
        fileField.setNullString("-");
        fileField.setPosition(-1);
        fileField.setLength(28);
        fileField.setPrecision(-1);
        fileField.setTrimType("none");
        fileField.setRepeated(false);
        result[5] = fileField;


        fileField = new BaseFileField();
        fileField.setName("gg");
        fileField.setType("String");
        fileField.setCurrencySymbol("￥");
        fileField.setDecimalSymbol(".");
        fileField.setGroupSymbol(",");
        fileField.setNullString("-");
        fileField.setPosition(-1);
        fileField.setLength(8);
        fileField.setPrecision(-1);
        fileField.setTrimType("none");
        fileField.setRepeated(false);
        result[6] = fileField;


        fileField = new BaseFileField();
        fileField.setName("hh");
        fileField.setType("Integer");
        fileField.setFormat("#");
        fileField.setCurrencySymbol("￥");
        fileField.setDecimalSymbol(".");
        fileField.setGroupSymbol(",");
        fileField.setNullString("-");
        fileField.setPosition(-1);
        fileField.setLength(15);
        fileField.setPrecision(0);
        fileField.setTrimType("none");
        fileField.setRepeated(false);
        result[7] = fileField;
        return result;
    }
}
