package com.alibaba.jvm.sandbox.api.listener.ext;

import com.alibaba.jvm.sandbox.api.resource.ModuleEventWatcher.Progress;

import java.lang.instrument.ClassFileTransformer;
import java.util.ArrayList;
import java.util.List;

/**
 * 事件观察附带信息
 *
 * @author luanjia@taobao.com
 * @since {@code sandbox-api:1.2.3}
 */
public class EventWatchWith {

    // 在事件完成编织之后，你可以通过这个类变型器来重新影响和对类进行变型
    private ClassFileTransformer afterClassFileTransformer;

    // 在事件编织过程中你可以观察到类渲染的进度
    private Progress progress;

    /**
     * 获取类渲染进度观察器
     *
     * @return 类渲染进度观察器
     */
    public Progress getProgress() {
        return progress;
    }

    /**
     * 设置类渲染进度观察器
     *
     * @param progress 类渲染进度观察器
     */
    public EventWatchWith setProgress(Progress progress) {
        this.progress = progress;
        return this;
    }

    /**
     * 获取编织后类变型器
     *
     * @return 编织后类变型器
     */
    public ClassFileTransformer getAfterClassFileTransformer() {
        return afterClassFileTransformer;
    }

    /**
     * 设置编织后类变型器
     *
     * @param afterClassFileTransformer 编织后类变型器
     */
    public EventWatchWith setAfterClassFileTransformer(ClassFileTransformer afterClassFileTransformer) {
        this.afterClassFileTransformer = afterClassFileTransformer;
        return this;
    }
}
