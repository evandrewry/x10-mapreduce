import x10.io.Console;
import x10.util.HashMap;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.Pair;
import x10.util.Timer;

/**
* this is the canonical multi-document word count example of a
* MapReduce program using our X10 flavor of MapReduce.
*/
public class WordCount {
    private static val timer = new Timer();

    /* MAPPER SUBCLASS */
    private static class WordCountMapper extends Mapper[Int, String, String, Int] {
        public def map(num:Int,
                       input:String,
                       output:MapperOutputCollector[String, Int]) {
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

    /* REDUCER SUBCLASS */
    private static class WordCountReducer extends Reducer[String, Int, String, Int] {
        public def reduce(key:String,
                          values:List[Int],
                          output:ReducerOutputCollector[String, Int]) {
            var sum:Int = 0;
            for (i in 0..(values.size() - 1)) {
                sum += values(i);
            }
            output.collect(key, sum);
        }
    }


    public static def main(argv:Array[String]{self.rank == 1}) {

        if (argv.size != 1) {                                                        
            Console.ERR.println("USAGE: WordCount <inputSize>");                     
            return;                                                                  
        }                                                                            
        val set_size = Long.parse(argv(0));


        /* set up input set */        
        val input = new ArrayList[Pair[Int, String]]();
        for (var i:Int = 0; i < set_size; i += 7) {
            input.add(Pair[Int, String](i + 1, str1));
            input.add(Pair[Int, String](i + 2, str2));
            input.add(Pair[Int, String](i + 3, str3));
            input.add(Pair[Int, String](i + 4, str4));
            input.add(Pair[Int, String](i + 5, str5));
            input.add(Pair[Int, String](i + 6, str6));
            input.add(Pair[Int, String](i + 7, str7));
        }

        
        /* set up job */        
        val mapper = new WordCountMapper();
        val reducer =  new WordCountReducer();
        val job = new MapReduceJob(mapper,
                                   reducer,
                                   (k:String, n:Int) => Math.abs(k.hashCode()) % n);


        /* run job */        
        val start = timer.milliTime();
        val output = job.run(input);
        val time = timer.milliTime() - start;
        Console.OUT.println("\t\t" + time);

        /* print results */
        //for (k in output.keySet())
        //Console.OUT.print("[" + k + " : " + output.get(k).value + "], ");
    }


    private static val str1 = "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a complete account of the system, and expound the actual teachings of the great explorer of the truth, the master-builder of human happiness. No one rejects, dislikes, or avoids pleasure itself, because it is pleasure, but because those who do not know how to pursue pleasure rationally encounter consequences that are extremely painful. Nor again is there anyone who loves or pursues or desires to obtain pain of itself, because it is pain, but occasionally circumstances occur in which toil and pain can procure him some great pleasure. To take a trivial example, which of us ever undertakes laborious physical exercise, except to obtain some advantage from it? But who has any right to find fault with a man who chooses to enjoy a pleasure that has no annoying consequences, or one who avoids a pain that produces no resultant pleasure? On the other hand, we denounce with righteous indignation and dislike men who are so beguiled and demoralized by the charms of pleasure of the moment, so blinded by desire, that they cannot foresee the pain and trouble that are bound to ensue; and equal blame belongs to those who fail in their duty through weakness of will, which is the same as saying through shrinking from toil and pain. These cases are perfectly simple and easy to distinguish. In a free hour, when our power of choice is untrammeled and when nothing prevents our being able to do what we like best, every pleasure is to be welcomed and every pain avoided. But in certain circumstances and owing to the claims of duty or the obligations of business it will frequently occur that pleasures have to be repudiated and annoyances accepted. The wise man therefore always holds in these matters to this principle of selection: he rejects pleasures to secure other greater pleasures, or else he endures pains to avoid worse pains.";

    private static val str2 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut dignissim iaculis ipsum eget molestie. Vestibulum non elit vel neque ultrices tempus sit amet a nisl. Ut sit amet velit a nunc blandit sagittis. Phasellus ultricies auctor eros in dignissim. Vestibulum sed urna sem. Fusce sit amet molestie purus. Phasellus bibendum mollis vestibulum. Vivamus sapien nunc, faucibus ut condimentum eget, pulvinar et arcu. Morbi vel elit at ante consequat rhoncus. Mauris in magna dapibus mauris egestas molestie. In mi nisl, fringilla vel pellentesque aliquam, hendrerit eu risus. Nam ac lacus laoreet massa sodales vulputate. Ut neque metus, scelerisque ut blandit mattis, egestas sed mauris. Donec porta dolor id tellus iaculis suscipit. Morbi eu nibh quis risus egestas lobortis quis id erat. Donec a nulla et ligula rhoncus consectetur at ut leo. Maecenas non semper orci. Nullam varius ultrices velit, nec vehicula lacus scelerisque vestibulum. Mauris id lorem in nunc suscipit aliquet imperdiet quis augue. Pellentesque auctor neque eu dolor molestie sodales. Integer scelerisque mollis sem, vitae pharetra felis viverra quis. In molestie ornare lectus eget mollis. Curabitur vel turpis elit, nec pharetra neque. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Cras imperdiet tellus vitae neque egestas bibendum. Nunc vulputate porta lacus, ut aliquet lorem volutpat ut. Nunc eu enim risus, eu pretium tellus. Donec justo metus, rhoncus sit amet imperdiet eu, mattis tristique ante. Etiam faucibus porttitor ante, vitae pharetra mi dictum quis. Aliquam eget faucibus lorem. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Nam vestibulum facilisis libero nec elementum. Nullam rutrum mauris vel mauris ornare tincidunt. Nulla blandit viverra egestas. Duis in eros rhoncus eros pretium malesuada. Aliquam commodo ante sit amet velit interdum sed adipiscing nibh bibendum. Aliquam felis neque, volutpat vel gravida non, consectetur ut est. Integer semper vehicula lorem, vel adipiscing turpis pretium a. Nullam a purus quis magna condimentum tempus. Fusce tortor mauris, mattis ac elementum id, molestie in leo. Vestibulum turpis odio, lobortis vitae aliquam vel, faucibus sit amet mi.";

    private static val str3 = "Nullam consectetur lacus pharetra elit tempor faucibus et vel lectus. Donec ut metus vitae mauris commodo faucibus. Sed mattis, tellus a accumsan eleifend, nisl urna viverra libero, sed pellentesque mi tellus ac orci. Donec erat nunc, fringilla quis consequat ac, molestie quis massa. Vestibulum eros turpis, sodales vitae egestas vitae, varius ut risus. Vestibulum eu elit risus. Pellentesque vel purus odio. Aliquam vel felis in est mollis dictum. Sed posuere vehicula dictum. Vivamus dictum tincidunt viverra. Aliquam et sem est, nec luctus ligula. Vestibulum ut dui erat, eget cursus orci. Quisque rutrum, lectus quis iaculis pharetra, turpis nulla iaculis nisi, sed convallis nunc arcu eu tortor.";

    private static val str4 = "Aliquam ac dolor quis lacus adipiscing auctor. Vivamus volutpat fringilla urna in vulputate. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Quisque faucibus placerat nibh ut sagittis. Nulla pretium fringilla odio tincidunt fermentum. Sed mattis felis sed quam tristique placerat. Nunc vel fermentum sapien. Aenean sit amet leo venenatis libero facilisis dictum eu vitae nibh. Vivamus at lectus eu diam laoreet gravida at a erat. Duis nulla est, facilisis sit amet gravida id, blandit a eros. Sed sollicitudin dictum tincidunt. Pellentesque dapibus vehicula lectus, vitae sollicitudin felis volutpat eget. Sed aliquet lectus diam, vel semper leo. Integer ornare, erat id accumsan elementum, mi quam varius leo, id luctus magna mauris in velit. Maecenas pulvinar, dui et mattis consequat, nisl velit dignissim nibh, sed viverra tortor nunc eget lacus. Morbi sem magna, vulputate et gravida eget, tincidunt eget neque. Integer et lacus et sapien viverra faucibus sed non metus. Cras a nisl purus. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Duis sed urna eu augue interdum imperdiet interdum nec felis. Curabitur aliquam eros id mi hendrerit vel lacinia velit pharetra. Sed quis placerat dolor. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Sed dictum, lectus nec feugiat feugiat, diam nisl tristique libero, eget gravida sapien dui venenatis quam. Mauris rhoncus elit rutrum ante elementum et venenatis felis laoreet. Fusce ullamcorper tincidunt ornare. Proin turpis magna, faucibus id rhoncus in, sagittis in eros. Quisque sollicitudin, massa id pretium iaculis, augue augue aliquam lorem, vel pulvinar nibh sapien eget enim. Aliquam eget tortor felis. Phasellus adipiscing ultricies ultricies. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Donec lobortis vestibulum augue nec commodo. Suspendisse adipiscing aliquam suscipit. Proin suscipit posuere nisl id tincidunt. Pellentesque sapien odio, aliquet interdum malesuada id, aliquam sit amet eros. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Curabitur quis lacus sit amet urna eleifend auctor. Praesent non quam quis sem volutpat adipiscing. Sed mollis lacus sit amet diam gravida tempor. Mauris vel libero massa, quis commodo odio. Cras nec quam mi, et condimentum dolor. Nunc fermentum dignissim turpis, sed porttitor ligula volutpat ac. Vivamus erat erat, dapibus vel lacinia ut, hendrerit id nunc. Nulla tempus dictum convallis. Donec rhoncus commodo nunc, et tristique augue semper ut. Curabitur ut erat nec sapien laoreet gravida. Suspendisse lacinia accumsan risus sed condimentum. Proin vel nulla eget nibh aliquet vehicula.";

    public static val str5 = "Integer in quam nisi. Nam lorem nisl, rhoncus vel interdum eu, tincidunt eget magna. Integer sed massa eros, sit amet fermentum metus. Integer semper risus nec ipsum sollicitudin nec dignissim est dignissim. Proin felis neque, adipiscing eu fermentum et, semper id risus. Proin vitae elit magna. Suspendisse bibendum, est et molestie iaculis, enim nisl condimentum neque, nec ultrices dui leo at velit. Donec vulputate dui in lacus semper ultrices. Ut ornare dapibus eros mattis porta. Vestibulum rhoncus orci eu nulla scelerisque imperdiet id eu enim. Phasellus rutrum luctus sem vel faucibus. Nunc dapibus, purus ut egestas molestie, massa orci bibendum risus, nec vehicula nisi tortor vitae odio. Nunc porttitor, enim sit amet pulvinar euismod, ipsum turpis consectetur mauris, vitae pellentesque lacus nibh in erat. Suspendisse a metus sed enim posuere euismod. Sed feugiat, elit ornare pretium euismod, est nisl placerat enim, ac placerat ipsum dolor at eros. Sed blandit tortor id nulla mollis scelerisque. Sed dapibus pellentesque orci molestie faucibus. Mauris et mi eros, ac scelerisque dolor. In tincidunt dui eu leo varius id posuere erat mollis. Aliquam fermentum consequat magna a dignissim. Aenean vel ligula vitae massa commodo vestibulum. Morbi sit amet orci ut sem facilisis rutrum vitae vitae ligula. Vivamus rhoncus facilisis laoreet. Duis sapien velit, pharetra a feugiat id, placerat eget risus. Nunc sodales elit ac orci convallis rhoncus. Donec interdum mollis lorem, scelerisque scelerisque lectus vehicula a. Cras semper interdum tempus. Nulla facilisi. Nulla purus ipsum, laoreet non pellentesque eget, iaculis at lectus. Donec quis nibh tortor. Mauris et dui eget nunc commodo ultrices. Nulla congue nisl quis ipsum blandit elementum. Integer lobortis tincidunt nisl, vel faucibus felis pretium eget. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Nullam vestibulum, lectus a posuere volutpat, est leo vulputate urna, ac fringilla nisl odio at sem. Aliquam pharetra pulvinar placerat.";

    private static val str6 = "Ut faucibus sapien sed tortor luctus condimentum. Nullam enim leo, euismod quis aliquam ut, pulvinar non nunc. Aliquam ac tortor in ante feugiat consequat at ac felis. Suspendisse potenti. Aenean placerat odio et lacus porta aliquam. Curabitur nec arcu eget massa porta suscipit in ut lorem. Suspendisse interdum, mi quis interdum commodo, nisl ligula laoreet sem, eget lacinia erat massa in velit. Donec enim turpis, commodo in bibendum non, pulvinar ut tortor. Cras imperdiet gravida magna ac consequat. Suspendisse nisi sapien, commodo id congue viverra, placerat volutpat risus. Phasellus id nibh et velit cursus consectetur. Vestibulum sit amet arcu a tellus porttitor tempus.";

    private static val str7 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc felis risus, interdum ultrices mattis id, convallis ut justo. Maecenas hendrerit lacus vitae metus semper vulputate semper ultrices eros. In ultrices semper molestie. Duis id risus vel mauris consectetur volutpat vel non mi. Mauris non metus nec erat vehicula posuere. Morbi vel justo vel ipsum ullamcorper feugiat et sed metus. Nam aliquam risus et eros imperdiet condimentum. Duis eget dignissim nisi. Sed consectetur malesuada erat. Pellentesque tortor libero, ultrices a convallis eget, feugiat eu nulla. Curabitur tortor est, imperdiet at bibendum quis, iaculis vel nibh. Aenean vestibulum facilisis est, ut luctus quam venenatis quis. Curabitur sollicitudin egestas arcu at accumsan. Ut condimentum dolor molestie tellus gravida hendrerit. Sed vel nulla lacus. Donec sed leo at nunc euismod hendrerit. Aliquam erat volutpat. Aenean et lacus non velit placerat facilisis suscipit non eros. Nullam ut sapien odio, vel hendrerit mauris. Fusce accumsan dolor vel nunc ultricies ultrices. In non mi mauris, nec feugiat magna. Sed tincidunt ornare eleifend. Integer in nulla sit amet arcu hendrerit pretium. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nam eu suscipit sem. Aenean vehicula rutrum imperdiet. Donec at enim massa, sit amet egestas elit. Suspendisse iaculis justo ut augue auctor scelerisque. Vivamus convallis feugiat eros nec suscipit. Donec sit amet urna nulla, vitae euismod lectus. Nunc pulvinar, eros in adipiscing tincidunt, turpis urna iaculis tortor, ac blandit libero lorem sit amet mi. Morbi condimentum velit quis nulla volutpat venenatis. Suspendisse sit amet sapien magna. Duis semper lobortis urna non tempus. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nulla tempor, mi quis egestas ultrices, lacus elit congue tellus, eu tincidunt nisi eros id nulla. Proin vulputate eros ut lectus elementum mattis. Duis non diam neque. Nam a adipiscing nisl. Vestibulum nec neque sit amet tellus iaculis vehicula. Aliquam dignissim cursus lacinia. Sed venenatis erat eu urna sodales vel porttitor elit facilisis. Fusce sed porta magna. Vestibulum orci dolor, facilisis at ornare id, sodales quis enim. Phasellus ornare accumsan suscipit. Donec tincidunt varius convallis. Vestibulum interdum ultrices elit, vitae pharetra eros tempor quis. Nulla interdum nibh ac purus pellentesque congue scelerisque leo ullamcorper. Proin quis rutrum urna. Nullam condimentum sagittis diam, at mattis quam posuere eu. Donec lacinia lacus eu nisi convallis eu suscipit leo rutrum. Pellentesque in nunc purus. Donec quis sem felis. Sed eget elit ac magna sagittis aliquam. Sed tellus massa, varius tristique bibendum sed, ultrices at augue. Sed convallis aliquam metus at vehicula. Morbi in libero tortor.";

}
