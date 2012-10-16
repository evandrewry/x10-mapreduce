public class WordCount {
    private class WordCountMapper extends Mapper[Int, String, String, Int] {
        def map(num:Int, text:String, oc:OutputCollector[String, Int]) {
            var word:String;
            while(text.trim().length() > 0) {
                text = text.trim();
                val end = indexOf(' ');
                word = end > 0 ? text.substring(0, end), text;
                oc.collect(word, 1);
            }
        }
    }
}
