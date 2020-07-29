package com.yuandaoma.code.kettleDemo;

import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputMeta;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.*;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.csvinput.CsvInputMeta;
import org.pentaho.di.trans.steps.file.BaseFileField;
import org.pentaho.di.trans.steps.file.BaseFileInputFiles;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;

public class CsvFileOutputTest {


    public static final String HDFS_PROTO = "hdfs://";
    public static final String HDFS_FILE = "a.csv";
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


            CsvInputMeta csvInputMeta = getCsvInputMeta();
            TextFileOutputMeta fileOutputMeta = new TextFileOutputMeta();
            fileOutputMeta.setFileName("aaa");
            fileOutputMeta.setEndedLine("\n");
            fileOutputMeta.setHeaderEnabled(true);
            fileOutputMeta.setSeparator(",");
            fileOutputMeta.setEnclosure("\"");
            fileOutputMeta.setExtension("txt");
            fileOutputMeta.setFileFormat("DOS");
            fileOutputMeta.setOutputFields(new TextFileField[]{});
            TransMeta transMeta = new TransMeta();
            StepMeta fromStep = new StepMeta("csvInput", csvInputMeta);
            fromStep.setDistributes(true);
            fromStep.setCopies(1);
            fromStep.setDraw(true);
            fromStep.setLocation(208,256);
            StepMeta toStep = new StepMeta("TextFileOutputMeta","TextFileOutputMeta", fileOutputMeta);
            toStep.setDistributes(true);
            toStep.setCopies(1);
            toStep.setDraw(true);
            toStep.setLocation(512,256);
            transMeta.addStep(fromStep);
            transMeta.addStep(toStep);
            TransHopMeta transHopMeta = new TransHopMeta();
            transHopMeta.setFromStep(fromStep);
            transHopMeta.setToStep(toStep);
            transHopMeta.setEnabled(true);
            transMeta.addTransHop(transHopMeta);
            transMeta.setName("csv-hadoop-file-output");
            String fileName = "T:\\test-a.ktr";
            String xml = transMeta.getXML();
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(fileName)));
            dos.write(xml.getBytes("UTF-8"));
            dos.close();
            System.out.println("保存完成");

            Trans trans = new Trans(transMeta);
            trans.setLogLevel(LogLevel.DEBUG);
            trans.execute(null);
            trans.waitUntilFinished();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void registerPlugin() {
        try {
            StepPluginType.getInstance().getPluginFolders().add(new PluginFolder("T:\\code-kettle\\plugins",
                    false,true));
            PluginRegistry registry = PluginRegistry.getInstance();
            Class<? extends PluginTypeInterface> pluginTypeClass = StepPluginType.class;
            Plugin hadoopMeta =new Plugin(new String[]{"HadoopFileOutputPlugin",}, pluginTypeClass, StepMetaInterface.class,
                    "Output", "HadoopFileOutputPlugin", "The GPLoad out step",
                    "/ui/images/HDI.png", false, true,new HashMap<Class<?>, String>(), new ArrayList<String>(),
                    null, // No error help file
                    null, // pluginFolder
                    null, // documentation URL
                    null, // cases URL
                    null // forum URL
            );
            registry.registerPlugin(pluginTypeClass, hadoopMeta);
        } catch (KettlePluginException e) {
            e.printStackTrace();
        }
    }

    private static void setHadoopOutputMetaProperties(HadoopFileOutputMeta hadoopFileOutputMeta) {
        hadoopFileOutputMeta.setSourceConfigurationName("node1");
        String date = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
        System.out.println("date = " + date);
        hadoopFileOutputMeta.setOutputFields(getTextOutputFiled());
        hadoopFileOutputMeta.setFileName("hdfs://192.168.20.200:8020/" + date + "/aa");
        hadoopFileOutputMeta.setDateInFilename(true);
        hadoopFileOutputMeta.setTimeInFilename(true);
//            hadoopFileOutputMeta.setFileName("./" + date + "/aa");
        hadoopFileOutputMeta.setExtension("txt");
        // 分割符
        hadoopFileOutputMeta.setSeparator(";");
        // 封闭符
        hadoopFileOutputMeta.setEnclosure("\"");
        // 编码
        hadoopFileOutputMeta.setEncoding("UTF-8");
        // 是否强制在字段周围加封闭符号
        hadoopFileOutputMeta.setEnclosureForced(false);
        // todo
        hadoopFileOutputMeta.setEnclosureFixDisabled(false);
        // 是否允许头部
        hadoopFileOutputMeta.setHeaderEnabled(false);
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
    }

    private static TextFileField[] getTextOutputFiled() {
        TextFileField[] textFileFields = new TextFileField[3];
        TextFileField element = new TextFileField();
        element.setName("id");
        element.setType(1);
        element.setFormat("#");
        element.setCurrencySymbol("￥");
        element.setPrecision(0);
        element.setTrimType(0);
        element.setLength(15);
        element.setDecimalSymbol(".");
        textFileFields[0] = element;
        element = new TextFileField();
        element.setName("name");
        element.setFormat("#");
        element.setType(1);
        element.setCurrencySymbol("￥");
        element.setPrecision(0);
        element.setTrimType(0);
        element.setLength(15);
        element.setDecimalSymbol(".");
        textFileFields[1] = element;

        element = new TextFileField();
        element.setName("age");
        element.setFormat("#");
        element.setType(1);
        element.setCurrencySymbol("￥");
        element.setPrecision(0);
        element.setTrimType(0);
        element.setLength(15);
        element.setDecimalSymbol(".");
        textFileFields[2] = element;
        return textFileFields;
    }

    private static CsvInputMeta getCsvInputMeta() {
        CsvInputMeta csvInputMeta = new CsvInputMeta();
        csvInputMeta.allocate(1);
        csvInputMeta.setFilename("T:\\a.csv");
        csvInputMeta.setEnclosure("\"");
        csvInputMeta.setBufferSize("5000");
        csvInputMeta.setHeaderPresent(true);
        csvInputMeta.setIncludingFilename(false);
        csvInputMeta.setDelimiter(",");
        csvInputMeta.setLazyConversionActive(true);
        csvInputMeta.setEncoding("UTF-8");
        TextFileInputField[] textFileFields = getTextFiled();


        csvInputMeta.setInputFields(textFileFields);
        return csvInputMeta;
    }

    private static TextFileInputField[] getTextFiled() {
        TextFileInputField[] textFileFields = new TextFileInputField[3];
        TextFileInputField element = new TextFileInputField();
        element.setName("id");
        element.setType(1);
        element.setFormat("#");
        element.setCurrencySymbol("￥");
        element.setPrecision(0);
        element.setTrimType(0);
        element.setLength(15);
        element.setDecimalSymbol(".");
        textFileFields[0] = element;
        element = new TextFileInputField();
        element.setName("name");
        element.setFormat("#");
        element.setType(1);
        element.setCurrencySymbol("￥");
        element.setPrecision(0);
        element.setTrimType(0);
        element.setLength(15);
        element.setDecimalSymbol(".");
        textFileFields[1] = element;

        element = new TextFileInputField();
        element.setName("age");
        element.setFormat("#");
        element.setType(1);
        element.setCurrencySymbol("￥");
        element.setPrecision(0);
        element.setTrimType(0);
        element.setLength(15);
        element.setDecimalSymbol(".");
        element.setGroupSymbol(",");
        textFileFields[2] = element;
        return textFileFields;
    }

    private static BaseFileInputFiles parseFiles() {

        BaseFileInputFiles files = new BaseFileInputFiles();
        files.fileName = new String[]{"hdfs://node1:8020/2007-10.csv"};
        files.includeSubFolders = new String[]{BaseFileInputFiles.NO};
        files.fileRequired = new String[]{BaseFileInputFiles.NO};
        files.acceptingFilenames = false;
        files.passingThruFields = false;
        return files;
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
