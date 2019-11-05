//package org.clever.canal.instance.manager.plain;
//
//import lombok.Getter;
//import lombok.NoArgsConstructor;
//import lombok.Setter;
//
//import java.util.Properties;
//
///**
// * plain远程配置，提供基于properties纯文本的配置
// */
//@NoArgsConstructor
//@Getter
//@Setter
//public class PlainCanal {
//
//    private Properties properties;
//    private String md5;
//    private String status;
//
//    @SuppressWarnings("WeakerAccess")
//    public PlainCanal(Properties properties, String status, String md5) {
//        this.properties = properties;
//        this.md5 = md5;
//        this.status = status;
//    }
//
//    @Override
//    public String toString() {
//        return "PlainCanal [properties=" + properties + ", md5=" + md5 + ", status=" + status + "]";
//    }
//}
