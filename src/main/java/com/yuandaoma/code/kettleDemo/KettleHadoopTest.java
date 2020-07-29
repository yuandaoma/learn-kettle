package com.yuandaoma.code.kettleDemo;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class KettleHadoopTest {
//    private static final String KETTLE_PLUGIN_BASE_FOLDER = "T:\\data-integration\\plugins";
    private static final String KETTLE_PLUGIN_BASE_FOLDER = "T:\\code-kettle\\src\\main\\resources\\.kettle\\plugins";

    public static void main(String[] args) throws Exception {
        // 这几句必须有, 官网例子是错的, 用来加载插件的
//        System.setProperty("hadoop.home.dir", "/");
        //kettle插件的位置

        StepPluginType.getInstance().getPluginFolders().add(new PluginFolder(KETTLE_PLUGIN_BASE_FOLDER,true, false));

        EnvUtil.environmentInit();
        KettleEnvironment.init();
        TransMeta transMeta = new TransMeta("T:\\Kettle-file\\kettle-hadoop.ktr");
        // 执行krt文件
        Trans trans = new Trans(transMeta);
        trans.execute(null);
        // 等待转换执行结束
        trans.waitUntilFinished();

        // 抛出异常
        if (trans.getErrors() > 0) {
            throw new KettleException("There are errors during transformation exception!(传输过程中发生异常)");
        }

    }
}
