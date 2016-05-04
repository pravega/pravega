import org.junit.*;
import static org.junit.Assert.*;
import java.util.*;

/**
 * @author mkyong
 *
 */
public class SampleTest {

    private Collection collection;

    @Test
    public static void oneTimeSetUp() {
        // one-time initialization code   
    	System.out.println("@BeforeClass - oneTimeSetUp");
    }
}
