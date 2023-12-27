package com.lostsidewalk.buffy.rule;

import com.lostsidewalk.buffy.rule.WebHookRequest.WebHookRequestException;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.*;

import static com.lostsidewalk.buffy.rule.WebHookRequest.WebHookRequestExceptionType.HTTP_CLIENT_ERROR;
import static com.lostsidewalk.buffy.rule.WebHookRequest.WebHookRequestExceptionType.HTTP_SERVER_ERROR;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

@Slf4j
class WebHookUtils {

    static void postWebHook(WebHookRequest webHookRequest, String userAgent) throws WebHookRequestException {
        String url = webHookRequest.url;
        log.debug("Performing webhook to URL={}", url);
        Integer statusCode = null;
        String statusMessage = null;
        try {
            // setup the initial connection
            HttpURLConnection feedConnection = openFeedConnection(url);
            // add authentication, if any
            addAuthenticator(feedConnection, webHookRequest.basicUsername, webHookRequest.basicPassword);
            // add the UA header
            feedConnection.setRequestProperty("User-Agent", userAgent);
            // add the CE header
            feedConnection.setRequestProperty("Content-Encoding", "application/json");
            // set the request method
            feedConnection.setRequestMethod("POST");
            // set doOutput
            feedConnection.setDoOutput(true);
            // send the payload
            Serializable payload = webHookRequest.getPayload();
            try (ObjectOutput objectOutputStream = new ObjectOutputStream(feedConnection.getOutputStream())) {
                objectOutputStream.writeObject(payload);
            }
            // get the status response
            statusCode = getStatusCode(feedConnection);
            // get the status message
            statusMessage = getStatusMessage(feedConnection);
            if (isClientError(statusCode)) { // otherwise, if this is a client error (4xx)
                // CLIENT_ERROR
                throw new WebHookRequestException(url, statusCode, statusMessage, HTTP_CLIENT_ERROR);
            } else if (isServerError(statusCode)) { // otherwise, if this is a server error (5xx)
                // SERVER_ERROR
                throw new WebHookRequestException(url, statusCode, statusMessage, HTTP_SERVER_ERROR);
            } // otherwise (this is a success response)
        } catch (@SuppressWarnings("OverlyBroadCatchBlock") IOException e) {
            throw new WebHookRequestException(url, statusCode, statusMessage, e);
        }
    }

    @SuppressWarnings("OverlyBroadThrowsClause") // MalformedURLException extends IOException
    private static HttpURLConnection openFeedConnection(String url) throws IOException {
        URL feedUrl = new URL(url);
        return (HttpURLConnection) feedUrl.openConnection();
    }

    private static void addAuthenticator(HttpURLConnection feedConnection, String username, String password) {
        if (username != null && password != null) {
            feedConnection.setAuthenticator(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray());
                }
            });
        }
    }

    private static int getStatusCode(HttpURLConnection feedConnection) throws IOException {
        feedConnection.setInstanceFollowRedirects(true);
        return feedConnection.getResponseCode();
    }

    private static String getStatusMessage(HttpURLConnection feedConnection) throws IOException {
        return feedConnection.getResponseMessage();
    }

    private static boolean isClientError(int statusCode) {
        return statusCode >= HTTP_BAD_REQUEST && statusCode < HTTP_INTERNAL_ERROR;
    }

    private static boolean isServerError(int statusCode) {
        return statusCode >= HTTP_INTERNAL_ERROR;
    }
}
