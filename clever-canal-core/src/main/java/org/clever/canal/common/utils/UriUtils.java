package org.clever.canal.common.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

@SuppressWarnings({"WeakerAccess", "unused"})
public final class UriUtils {
    private final static String SPLIT = "&";
    private final static String EQUAL = "=";
    private final static String DEFAULT_ENCODING = "ISO_8859_1";

    private UriUtils() {
    }

    public static Map<String, String> parseQuery(final String uriString) {
        URI uri;
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return parseQuery(uri);
    }

    public static Map<String, String> parseQuery(final String uriString, final String encoding) {
        URI uri;
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return parseQuery(uri, encoding);
    }

    public static Map<String, String> parseQuery(final URI uri) {
        return parseQuery(uri, DEFAULT_ENCODING);
    }

    public static Map<String, String> parseQuery(final URI uri, final String encoding) {
        if (uri == null || StringUtils.isBlank(uri.getQuery())) {
            return Collections.emptyMap();
        }
        String query = uri.getRawQuery();
        HashMap<String, String> params = new HashMap<>();
        @SuppressWarnings("resource")
        Scanner scan = new Scanner(query);
        scan.useDelimiter(SPLIT);
        while (scan.hasNext()) {
            String token = scan.next().trim();
            String[] pair = token.split(EQUAL);
            String key = decode(pair[0], encoding);
            String value = null;
            if (pair.length == 2) {
                value = decode(pair[1], encoding);
            }
            params.put(key, value);
        }
        return params;
    }

    private static String decode(final String content, final String encoding) {
        try {
            return URLDecoder.decode(content, encoding != null ? encoding : DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
