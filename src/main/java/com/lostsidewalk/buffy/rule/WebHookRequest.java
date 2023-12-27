package com.lostsidewalk.buffy.rule;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLHandshakeException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

@Slf4j
@Data
@AllArgsConstructor
class WebHookRequest {

    String url;

    Serializable payload;

    String basicUsername;

    String basicPassword;

    public enum WebHookRequestExceptionType {
        FILE_NOT_FOUND_EXCEPTION,
        SSL_HANDSHAKE_EXCEPTION,
        IO_EXCEPTION,
        UNKNOWN_HOST_EXCEPTION,
        SOCKET_TIMEOUT_EXCEPTION,
        SOCKET_EXCEPTION,
        CONNECT_EXCEPTION,
        ILLEGAL_ARGUMENT_EXCEPTION,
        HTTP_CLIENT_ERROR,
        HTTP_SERVER_ERROR,
        OTHER
    }

    static class WebHookRequestException extends Exception {
        @Serial
        private static final long serialVersionUID = 98509623048523L;
        final String feedUrl;
        final Integer httpStatusCode;
        final String httpStatusMessage;
        final WebHookRequestExceptionType exceptionType;

        WebHookRequestException(String feedUrl, Integer httpStatusCode, String httpStatusMessage, WebHookRequestExceptionType exceptionType) {
            super(exceptionType.name());
            this.feedUrl = feedUrl;
            this.httpStatusCode = httpStatusCode;
            this.httpStatusMessage = httpStatusMessage;
            this.exceptionType = exceptionType;
        }

        WebHookRequestException(String feedUrl, Integer httpStatusCode, String httpStatusMessage, Exception exception) {
            super(exception.getMessage(), exception);
            this.feedUrl = feedUrl;
            this.httpStatusCode = httpStatusCode;
            this.httpStatusMessage = httpStatusMessage;
            //noinspection IfStatementWithTooManyBranches,ChainOfInstanceofChecks
            if (exception instanceof FileNotFoundException) {
                exceptionType = WebHookRequestExceptionType.FILE_NOT_FOUND_EXCEPTION;
            } else if (exception instanceof SSLHandshakeException) {
                exceptionType = WebHookRequestExceptionType.SSL_HANDSHAKE_EXCEPTION;
            } else if (exception instanceof UnknownHostException) {
                exceptionType = WebHookRequestExceptionType.UNKNOWN_HOST_EXCEPTION;
            } else if (exception instanceof SocketTimeoutException) {
                exceptionType = WebHookRequestExceptionType.SOCKET_TIMEOUT_EXCEPTION;
            } else if (exception instanceof ConnectException) {
                exceptionType = WebHookRequestExceptionType.CONNECT_EXCEPTION;
            } else if (exception instanceof SocketException) {
                exceptionType = WebHookRequestExceptionType.SOCKET_EXCEPTION;
            } else if (exception instanceof IllegalArgumentException) {
                exceptionType = WebHookRequestExceptionType.ILLEGAL_ARGUMENT_EXCEPTION;
            } else if (exception instanceof IOException) {
                exceptionType = WebHookRequestExceptionType.IO_EXCEPTION;
            } else {
                exceptionType = WebHookRequestExceptionType.OTHER;
            }
        }
    }
}
