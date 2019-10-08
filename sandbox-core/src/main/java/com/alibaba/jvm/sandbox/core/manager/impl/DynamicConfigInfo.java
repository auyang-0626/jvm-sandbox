/*
 * @Author: yanggy
 * @Description: ~~
 * @Date: Create in 5:37 下午 2019/9/3
 */

package com.alibaba.jvm.sandbox.core.manager.impl;

import com.alibaba.jvm.sandbox.core.CoreConfigure;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

public class DynamicConfigInfo extends DefaultConfigInfo{

    private final Logger logger = LoggerFactory.getLogger(getClass());


    public static final String RootKey = "pts_";
    public static final String RegisterKey = RootKey+"register";

    private JedisPool jedisPool;
    private CoreConfigure coreConfigure;

    private static DynamicConfigInfo configInfo;

    private LoadingCache<Pair<String,String>, String> cache = CacheBuilder.newBuilder().expireAfterWrite(120, TimeUnit.SECONDS).build(
            new CacheLoader<Pair<String,String>, String>() {
                @Override
                public String load(final Pair<String,String> key) throws Exception {
                    return invoke(new Operate() {
                        @Override
                        public String invoke(Jedis jedis) {
                            return jedis.hget(DynamicConfigInfo.RootKey + key.getKey(), key.getValue());
                        }
                    });
                }
            }
    );

    private DynamicConfigInfo(CoreConfigure cfg) {
        super(cfg);
        try {
            this.coreConfigure = cfg;
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxIdle(1);
            jedisPoolConfig.setMaxWaitMillis(1000L);

            if (coreConfigure.isProd()){
                jedisPool = new JedisPool(jedisPoolConfig,"r-bp18de0ae2e29fb4.redis.rds.aliyuncs.com",6379,1000,"3DNQvSy3y4igGc2P");
            }else {
                jedisPool = new JedisPool(jedisPoolConfig,"cache.redis.scsite.net",6379,1000);
            }
        }catch (Exception e){
            logger.error("DynamicConfigInfo创建失败！",e);
        }

    }
    public static DynamicConfigInfo getInstance(CoreConfigure cfg){
        if(configInfo == null){
            synchronized (DynamicConfigInfo.class){
                if(configInfo == null){
                    configInfo = new DynamicConfigInfo(cfg);
                }
            }
        }
        return configInfo;
    }


    public void register(final String hostName, final int port) {
        invoke(new Operate() {
            @Override
            public String invoke(Jedis jedis) {
                String ip = hostName;
                try {
                    ip = InetAddress.getLocalHost().getHostAddress();
                }catch (Exception e){
                    //ignore
                }
                jedis.hset(RegisterKey, coreConfigure.getAppKey() + "_" + ip, String.valueOf(port));
                return "success";
            }
        });
    }
    public String invoke(Operate operate){
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return operate.invoke(jedis);
        }catch (Exception e){
            logger.error("configserver invoke error!",e);
        }finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    @Override
    public String getDynamicValue(String key, String field) {
        try {
            return cache.get(new Pair<String, String>(key, field));
        } catch (Exception e) {
            return null;
        }
    }


    public CoreConfigure getCoreConfigure() {
        return coreConfigure;
    }

    public interface Operate{
        String invoke(Jedis jedis);
    }
}
