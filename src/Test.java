import sun.misc.Unsafe;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

class Test {
    public static void main(String[] args) throws FileNotFoundException {
        FileSplitAndMerge my = new FileSplitAndMerge();
        List<String> partition = my.partition(new File("demo.txt"));
        my.merge(partition);
        return;
    }

}


