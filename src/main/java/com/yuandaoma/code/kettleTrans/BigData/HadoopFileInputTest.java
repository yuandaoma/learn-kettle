package com.yuandaoma.code.kettleTrans.BigData;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class HadoopFileInputTest {

    public static void main(String[] args) throws Exception {
        //kettle插件的位置
        EnvUtil.environmentInit();
        KettleEnvironment.init();
        TransMeta transMeta = new TransMeta("T:\\Kettle-file\\hadoop-file-input-test.ktr");
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
