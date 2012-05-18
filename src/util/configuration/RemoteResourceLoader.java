package util.configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * RemoteResourceLoader loads remotely-stored resources. It can load resources
 * as raw strings, JSON objects, or JSON arrays.
 * 
 * TODO: This doesn't really belong in "util.configuration" because it's not
 * related to the project's local configuration, but it seems like a reasonable
 * place to put it for now because it's near LocalResourceLoader.
 * 
 * @author Sam
 */
public class RemoteResourceLoader {
    public static JSONObject loadJSON(String theURL) throws IOException {
        try {
            return new JSONObject(loadRaw(theURL));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    
    public static JSONArray loadJSONArray(String theURL) throws IOException {
        try {
            return new JSONArray(loadRaw(theURL));
        } catch (Exception e) {
            throw new IOException(e);
        }                
    }
    
    public static String loadRaw(String theURL) throws IOException {
        URL url = new URL(theURL);
        URLConnection urlConnection = url.openConnection();                
        if (urlConnection.getContentLength() == 0)
            throw new IOException("Could not load URL: " + theURL);
        StringBuilder theJSON = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
        do {
            String nextLine = br.readLine();
            if (nextLine == null) break;
            theJSON.append(nextLine);
        } while (true);
        return theJSON.toString();
    }

    public static String postRawWithTimeout(String theURL, String toPost, int nTimeout) throws IOException {
        URL url = new URL(theURL);
        URLConnection urlConnection = url.openConnection();
        urlConnection.setDoOutput(true);
        urlConnection.setConnectTimeout(nTimeout);
        OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
        out.write(toPost);
        out.close();
        
        BufferedReader in = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
        StringBuilder decodedString = new StringBuilder();
        String decodedLine;
        while ((decodedLine = in.readLine()) != null) {
            decodedString.append(decodedLine);
        }
        in.close();
        return decodedString.toString();
    }
}