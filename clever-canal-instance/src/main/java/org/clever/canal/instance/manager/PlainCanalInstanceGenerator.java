//package org.clever.canal.instance.manager;
//
//import org.clever.canal.common.CanalException;
//import org.clever.canal.instance.core.CanalInstance;
//import org.clever.canal.instance.core.CanalInstanceGenerator;
//import org.clever.canal.instance.manager.plain.PlainCanal;
//import org.clever.canal.instance.manager.plain.PlainCanalConfigClient;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Properties;
//
////import org.springframework.beans.factory.BeanFactory;
////import org.springframework.context.ApplicationContext;
////import org.springframework.context.support.ClassPathXmlApplicationContext;
//
///** TODO lzw
// * 基于manager生成对应的{@linkplain CanalInstance}
// */
//public class PlainCanalInstanceGenerator implements CanalInstanceGenerator {
//
//    private static final Logger logger = LoggerFactory.getLogger(SpringCanalInstanceGenerator.class);
//    private String springXml;
//    private PlainCanalConfigClient canalConfigClient;
//    private String defaultName = "instance";
//    private BeanFactory beanFactory;
//    private Properties canalConfig;
//
//    public PlainCanalInstanceGenerator(Properties canalConfig) {
//        this.canalConfig = canalConfig;
//    }
//
//    public CanalInstance generate(String destination) {
//        synchronized (CanalInstanceGenerator.class) {
//            try {
//                PlainCanal canal = canalConfigClient.findInstance(destination, null);
//                if (canal == null) {
//                    throw new CanalException("instance : " + destination + " config is not found");
//                }
//                Properties properties = canal.getProperties();
//                // merge local
//                properties.putAll(canalConfig);
//
//                // 设置动态properties,替换掉本地properties
//                com.alibaba.otter.canal.instance.spring.support.PropertyPlaceholderConfigurer.propertiesLocal.set(properties);
//                // 设置当前正在加载的通道，加载spring查找文件时会用到该变量
//                System.setProperty("canal.instance.destination", destination);
//                this.beanFactory = getBeanFactory(springXml);
//                String beanName = destination;
//                if (!beanFactory.containsBean(beanName)) {
//                    beanName = defaultName;
//                }
//
//                return (CanalInstance) beanFactory.getBean(beanName);
//            } catch (Throwable e) {
//                logger.error("generator instance failed.", e);
//                throw new CanalException(e);
//            } finally {
//                System.setProperty("canal.instance.destination", "");
//            }
//        }
//    }
//
//    // ================ setter / getter ================
//
//    private BeanFactory getBeanFactory(String springXml) {
//        ApplicationContext applicationContext = new ClassPathXmlApplicationContext(springXml);
//        return applicationContext;
//    }
//
//    public void setCanalConfigClient(PlainCanalConfigClient canalConfigClient) {
//        this.canalConfigClient = canalConfigClient;
//    }
//
//    public void setSpringXml(String springXml) {
//        this.springXml = springXml;
//    }
//
//}
