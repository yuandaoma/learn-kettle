package com.yuandaoma.code.kettleTrans.BigData;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class HadoopFileOutputTest {
    private static final String KETTLE_PLUGIN_BASE_FOLDER = "T:\\data-integration\\plugins";

    public static void main(String[] args) throws Exception {
        // 这几句必须有, 官网例子是错的, 用来加载插件的
//        System.setProperty("hadoop.home.dir", "/");
        //kettle插件的位置

//        StepPluginType.getInstance().getPluginFolders().add(new PluginFolder(KETTLE_PLUGIN_BASE_FOLDER,true, true));

//        StepPluginType.getInstance().getPluginFolders().add(new PluginFolder("T:\\data-integration\\plugins\\pentaho-big-data-plugin",
//                false,true));
        StepPluginType.getInstance().getPluginFolders().add(new PluginFolder("T:\\code-kettle\\plugins",
                true,true));
//        String KETTLE_PLUGIN_BASE_FOLDER = "T:\\data-integration\\plugins";
        String KETTLE_PLUGIN_BASE_FOLDER = "T:\\code-kettle\\plugins";
        System.setProperty("KETTLE_PLUGIN_BASE_FOLDERS", KETTLE_PLUGIN_BASE_FOLDER);
        EnvUtil.environmentInit();
        KettleEnvironment.init();




        TransMeta transMeta = new TransMeta("T:\\Kettle-file\\hadoop-file-output-test.ktr");
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
