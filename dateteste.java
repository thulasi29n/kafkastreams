import java.text.SimpleDateFormat;
import java.util.Date;

public class DateToEpochExample {
    public static void main(String[] args) {
        String dateString = "30-DEC-2022 18.25.22.124";

        SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy HH.mm.ss.SSS");
        try {
            Date date = sdf.parse(dateString);
            long epochTime = date.getTime();
            String epochString = String.valueOf(epochTime);
            
            System.out.println("Epoch Time: " + epochString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
