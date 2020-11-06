
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FileSplitAndMerge
{
    private static ExecutorService executorService = Executors.newFixedThreadPool(3);

    private static int SPILT_SIZE = 1024; // 按照1KB粒度切割文件

    public List<String> partition(File file) {
        long fileLength = file.length();
        int partCount = (int) (fileLength / SPILT_SIZE);
        if (fileLength % SPILT_SIZE != 0) {
            partCount++;
        }

        List<String> fileList = new ArrayList<String>();

        for (int i = 0; i < partCount; i++) {
            int startPos = i * SPILT_SIZE;
            long curSize = (i + 1 == partCount) ? (fileLength - startPos) : SPILT_SIZE;

            String partFileName = file.getName() + "." + (i+1) + ".part";
            fileList.add(partFileName);

            executorService.execute(new SplitThread(partFileName, startPos, Integer.valueOf(curSize + ""),file));//并行
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                executorService.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return fileList;
    }
    public void merge(List<String> fileList) {
        RandomAccessFile randomAccessFile = null;
        byte[] b = new byte[SPILT_SIZE * 2];
        FileInputStream fileInputStream = null;
        long offset = 0L;
        int len = 0;
        try {
            randomAccessFile = new RandomAccessFile("mergeFile.txt", "rw");
            for (String filePart : fileList) {
                randomAccessFile.seek(offset);
                fileInputStream = new FileInputStream(filePart);
                int fileSize = 0;
                while ((len = fileInputStream.read(b)) > 0) {
                    fileSize+=len;
                    randomAccessFile.write(b, 0, len);
                }
                fileInputStream.close();
                offset = offset + fileSize;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return;
    }


}
class SplitThread implements Runnable
{
    private int startPos;
    private long curSize;
    private File parentFile;
    private String fileName;
    public SplitThread(String fileName,int startPos, long curSize,File file){
        this.parentFile = file;
        this.startPos = startPos;
        this.curSize = curSize;
        this.fileName = fileName;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + "  filename: " + this.fileName + ", startPos: " + this.startPos);
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            inputStream = new FileInputStream(this.parentFile);
            inputStream.skip(this.startPos);
            outputStream = new FileOutputStream(new File(this.fileName));
            byte[] buffer = new byte[(int)this.curSize];
            inputStream.read(buffer, 0, (int)this.curSize);
            String title  = "\n"+Thread.currentThread().getName()+": ";
            outputStream.write(title.getBytes());  //写入头
            outputStream.write(buffer);
            outputStream.flush();
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}