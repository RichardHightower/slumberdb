package info.slumberdb;

import static org.boon.Boon.puts;

public class Client  {

    public void put(Entry<String, String> entry) {

        puts(entry);
    }


    public String get(String key) {
        return key + ".mommy";
    }


}
