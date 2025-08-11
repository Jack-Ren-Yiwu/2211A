package util;
import com.huaban.analysis.jieba.JiebaSegmenter;

import java.util.List;

public class KeywordUtil {
    public static void main(String[] args) {
        JiebaSegmenter segmenter = new JiebaSegmenter();
        String text = "这是一个中文分词的例子。";
        List<String> segments = segmenter.sentenceProcess(text);
        System.out.println(segments);
    }
}
