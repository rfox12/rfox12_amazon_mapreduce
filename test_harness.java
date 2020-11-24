import java.util.regex.*;
import com.google.gson.*;

public class MyClass {
    public static void main(String args[]) {
        // This is a PRODUCT json string (use this jsonString if working on products)
        String jsonString = "{\"category\": [\"Electronics\", \"Camera &amp; Photo\", \"Video Surveillance\", \"Surveillance Systems\", \"Surveillance DVR Kits\"], \"tech1\": \"\", \"description\": [\"The following camera brands and models have bee\r\nn tested for compatibility with GV-Software.\\nGeoVision \\tACTi \\tArecont Vision \\tAXIS \\tBosch \\tCanon\\nCNB \\tD-Link \\tEtroVision \\tHikVision \\tHUNT \\tIQEye\\nJVC \\tLG \\tMOBOTIX \\tPanasonic \\tPelco \\tS\r\namsung\\nSanyo \\tSony \\tUDP \\tVerint \\tVIVOTEK \\t \\n \\nCompatible Standard and Protocol\\nGV-System also allows for integration with all other IP video devices compatible with ONVIF(V2.0), PSIA (V1.1) s\r\ntandards, or RTSP protocol.\\nONVIF \\tPSIA \\tRTSP \\t  \\t  \\t \\nNote: Specifications are subject to change without notice. Every effort has been made to ensure that the information on this Web site is a\r\nccurate. No liability is assumed for incidental or consequential damages arising from the use of the information or products contained herein.\"], \"fit\": \"\", \"title\": \"Genuine Geovision 1 Channel 3rd P\r\narty NVR IP Software with USB Dongle Onvif PSIA\", \"also_buy\": [], \"image\": [\"https://images-na.ssl-images-amazon.com/images/I/411uoWa89KL._SS40_.jpg\"], \"tech2\": \"\", \"brand\": \"GeoVision\", \"feature\": [\"\r\nGenuine Geovision 1 Channel NVR IP Software\", \"Support 3rd Party IP Camera\", \"USB Dongle\"], \"rank\": [\">#3,092 in Tools &amp; Home Improvement &gt; Safety &amp; Security &gt; Home Security &amp; Survei\r\nllance &gt; Complete Surveillance Systems &gt; Surveillance DVR Kits\", \">#5,010 in Tools &amp; Home Improvement &gt; Safety &amp; Security &gt; Home Security &amp; Surveillance &gt; Surveillance Video\r\n Equipment\"], \"also_view\": [], \"main_cat\": \"Camera &amp; Photo\", \"similar_item\": \"\", \"date\": \"January 28, 2014\", \"price\": \"$65.00\", \"asin\": \"0011300000\"}";
        
        // This is a REVIEW json string (use this jsonString if working on reviews)
        // String jsonString = "{\"overall\": 1.0, \"vote\": \"25\", \"verified\": false, \"reviewTime\": \"12 19, 2008\", \"reviewerID\": \"APV13CM0919JD\", \"asin\": \"B001GXRQW0\", \"style\": {\"Gift Amount:\": \" 50\"}, \"reviewerName\": \"LEH\", \"reviewText\": \"Amazon,\\nI am shopping for Amazon.com gift cards for Christmas gifts and am really so disappointed that out of five choices there isn't one that says \\\"Merry Christmas\\\" or mentions Christmas at all!  I am sure I am not alone in wanting a card that reflects the actual \\\"holiday\\\" we are celebrating. On principle, I cannot send a Amazon gift card this Christmas.  What's up with all the Political Correctness?  Bad marketing decision.\\nLynn\", \"summary\": \"Merry Christmas.\", \"unixReviewTime\": 1229644800}";
        
        JsonParser parser = new JsonParser();
        JsonElement jsonTree = parser.parse(jsonString);
        JsonObject jsonObject = jsonTree.getAsJsonObject();
            
        // Product code example ==================================================        
        String maincat = jsonObject.get("main_cat").getAsString();
        if (maincat.startsWith("<")) {
            String pattern = ".*alt=\"([^\"]*)\".*";
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher(maincat);
            while(m.find()) { maincat = m.group(1); }
        }
        
        System.out.println("Category is: " + maincat);
        
        // Review code example ==================================================        
        // String reviewrID = jsonObject.get("reviewerID").getAsString();
        // System.out.println("Reviewer ID is: " + reviewrID);
    }
}
