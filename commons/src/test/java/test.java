import org.junit.Test;

import java.util.Random;

public class test {
    
    @Test
    public void random(){
        for (int i = 0;i < 100;i++){
            System.out.println(new Random().nextInt(22) + 1);
        }
    }
    
}
