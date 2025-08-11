package util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @Time : 2022/11/15 13:11
 * @Author :tzh
 * @File : TransientSink
 * @Project : gmall_flink
 **/
@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
