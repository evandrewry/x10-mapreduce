import x10.io.Console;
import x10.util.HashMap;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.Box;

public class WordCount {

    private static class WordCountMapper extends Mapper[Int, String, String, Int] {
        public def map(num:Int,
                       input:String,
                       output:OutputCollector[String, Int]) {
            var text:String = input;
            var word:String;
            while(text.trim().length() > 0) {
                text = text.trim();
                val end = text.indexOf(' ');
                if (end > 0) {
                    word = text.substring(0, end);
                    text = text.substring(end);
                } else {
                    word = text;
                    text = "";
                }
                output.collect(word, 1);
            }
        }
    }

    private static class WordCountReducer extends Reducer[String, Int, String, Int] {
        public def reduce(key:String,
                          values:List[Int],
                          output:OutputCollector[String, Int]) {
            var sum:Int = 0;
            for (i in 0..(values.size() - 1)) {
                sum += values(i);
            }
            output.collect(key, sum);
        }
    }

    private static class WordCountMapperOutputCollector extends OutputCollector[String, Int] {
        public def collect(key:String, value:Int) {
            var current:Box[Int] = get(key);
            if (current != null) {
                put(key, current.value + 1);
            } else {
                put(key, 1);
            }
        }
    }

    public static def main(args:Array[String]{self.rank == 1}) {


        val input_set_1 = new HashMap[Int, String]();
        input_set_1.put(1, str1);
        input_set_1.put(2, str2);
        input_set_1.put(3, str3);

        val input_set_2 = new HashMap[Int, String]();        
        input_set_2.put(1, str4);
        input_set_2.put(2, str5);
        input_set_2.put(3, str6);

        val input = new ArrayList[HashMap[Int, String]]();        
        input.add(input_set_1);
        input.add(input_set_2);

        val mapper = new WordCountMapper();
        val reducer =  new WordCountReducer();
        val map_output_collector = new WordCountMapperOutputCollector();
        val reduce_output_collector = new OutputCollector[String, Int]();
        val wordCounter = new MapReduceJob(mapper,
                                           reducer,
                                           map_output_collector,
                                           reduce_output_collector);

        val output = wordCounter.run(input);

        for (k in output.keySet())
            Console.OUT.println(k + " : " + output.get(k).value);
    }


    private static val str1 = "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a complete account of the system, and expound the actual teachings of the great explorer of the truth, the master-builder of human happiness. No one rejects, dislikes, or avoids pleasure itself, because it is pleasure, but because those who do not know how to pursue pleasure rationally encounter consequences that are extremely painful. Nor again is there anyone who loves or pursues or desires to obtain pain of itself, because it is pain, but occasionally circumstances occur in which toil and pain can procure him some great pleasure. To take a trivial example, which of us ever undertakes laborious physical exercise, except to obtain some advantage from it? But who has any right to find fault with a man who chooses to enjoy a pleasure that has no annoying consequences, or one who avoids a pain that produces no resultant pleasure? On the other hand, we denounce with righteous indignation and dislike men who are so beguiled and demoralized by the charms of pleasure of the moment, so blinded by desire, that they cannot foresee the pain and trouble that are bound to ensue; and equal blame belongs to those who fail in their duty through weakness of will, which is the same as saying through shrinking from toil and pain. These cases are perfectly simple and easy to distinguish. In a free hour, when our power of choice is untrammeled and when nothing prevents our being able to do what we like best, every pleasure is to be welcomed and every pain avoided. But in certain circumstances and owing to the claims of duty or the obligations of business it will frequently occur that pleasures have to be repudiated and annoyances accepted. The wise man therefore always holds in these matters to this principle of selection: he rejects pleasures to secure other greater pleasures, or else he endures pains to avoid worse pains.";

    private static val str2 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut dignissim iaculis ipsum eget molestie. Vestibulum non elit vel neque ultrices tempus sit amet a nisl. Ut sit amet velit a nunc blandit sagittis. Phasellus ultricies auctor eros in dignissim. Vestibulum sed urna sem. Fusce sit amet molestie purus. Phasellus bibendum mollis vestibulum. Vivamus sapien nunc, faucibus ut condimentum eget, pulvinar et arcu. Morbi vel elit at ante consequat rhoncus.";

    public static val str3 = "Nullam consectetur lacus pharetra elit tempor faucibus et vel lectus. Donec ut metus vitae mauris commodo faucibus. Sed mattis, tellus a accumsan eleifend, nisl urna viverra libero, sed pellentesque mi tellus ac orci. Donec erat nunc, fringilla quis consequat ac, molestie quis massa. Vestibulum eros turpis, sodales vitae egestas vitae, varius ut risus. Vestibulum eu elit risus. Pellentesque vel purus odio. Aliquam vel felis in est mollis dictum. Sed posuere vehicula dictum. Vivamus dictum tincidunt viverra. Aliquam et sem est, nec luctus ligula. Vestibulum ut dui erat, eget cursus orci. Quisque rutrum, lectus quis iaculis pharetra, turpis nulla iaculis nisi, sed convallis nunc arcu eu tortor.";

    public static val str4 = "Aliquam ac dolor quis lacus adipiscing auctor. Vivamus volutpat fringilla urna in vulputate. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Quisque faucibus placerat nibh ut sagittis. Nulla pretium fringilla odio tincidunt fermentum. Sed mattis felis sed quam tristique placerat. Nunc vel fermentum sapien. Aenean sit amet leo venenatis libero facilisis dictum eu vitae nibh. Vivamus at lectus eu diam laoreet gravida at a erat. Duis nulla est, facilisis sit amet gravida id, blandit a eros. Sed sollicitudin dictum tincidunt. Pellentesque dapibus vehicula lectus, vitae sollicitudin felis volutpat eget. Sed aliquet lectus diam, vel semper leo. Integer ornare, erat id accumsan elementum, mi quam varius leo, id luctus magna mauris in velit.";

    public static val str5 = "Integer in quam nisi. Nam lorem nisl, rhoncus vel interdum eu, tincidunt eget magna. Integer sed massa eros, sit amet fermentum metus. Integer semper risus nec ipsum sollicitudin nec dignissim est dignissim. Proin felis neque, adipiscing eu fermentum et, semper id risus. Proin vitae elit magna. Suspendisse bibendum, est et molestie iaculis, enim nisl condimentum neque, nec ultrices dui leo at velit. Donec vulputate dui in lacus semper ultrices. Ut ornare dapibus eros mattis porta. Vestibulum rhoncus orci eu nulla scelerisque imperdiet id eu enim. Phasellus rutrum luctus sem vel faucibus. Nunc dapibus, purus ut egestas molestie, massa orci bibendum risus, nec vehicula nisi tortor vitae odio.";

    public static val str6 = "Ut faucibus sapien sed tortor luctus condimentum. Nullam enim leo, euismod quis aliquam ut, pulvinar non nunc. Aliquam ac tortor in ante feugiat consequat at ac felis. Suspendisse potenti. Aenean placerat odio et lacus porta aliquam. Curabitur nec arcu eget massa porta suscipit in ut lorem. Suspendisse interdum, mi quis interdum commodo, nisl ligula laoreet sem, eget lacinia erat massa in velit. Donec enim turpis, commodo in bibendum non, pulvinar ut tortor. Cras imperdiet gravida magna ac consequat. Suspendisse nisi sapien, commodo id congue viverra, placerat volutpat risus. Phasellus id nibh et velit cursus consectetur. Vestibulum sit amet arcu a tellus porttitor tempus.";
}
