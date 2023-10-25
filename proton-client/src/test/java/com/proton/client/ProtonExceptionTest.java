package com.proton.client;

import java.util.concurrent.ExecutionException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonExceptionTest {
    @Test(groups = { "unit" })
    public void testConstructorWithCause() {
        ProtonException e = new ProtonException(-1, (Throwable) null, null);
        Assert.assertEquals(e.getErrorCode(), -1);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error -1");

        ProtonNode server = ProtonNode.builder().build();
        e = new ProtonException(233, (Throwable) null, server);
        Assert.assertEquals(e.getErrorCode(), 233);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error 233, server " + server);

        Throwable cause = new IllegalArgumentException();
        e = new ProtonException(123, cause, server);
        Assert.assertEquals(e.getErrorCode(), 123);
        Assert.assertEquals(e.getCause(), cause);
        Assert.assertEquals(e.getMessage(), "Unknown error 123, server " + server);

        cause = new IllegalArgumentException("Some error");
        e = new ProtonException(111, cause, server);
        Assert.assertEquals(e.getErrorCode(), 111);
        Assert.assertEquals(e.getCause(), cause);
        Assert.assertEquals(e.getMessage(), "Some error, server " + server);
    }

    @Test(groups = { "unit" })
    public void testConstructorWithoutCause() {
        ProtonException e = new ProtonException(-1, (String) null, null);
        Assert.assertEquals(e.getErrorCode(), -1);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error -1");

        ProtonNode server = ProtonNode.builder().build();
        e = new ProtonException(233, (String) null, server);
        Assert.assertEquals(e.getErrorCode(), 233);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error 233, server " + server);

        e = new ProtonException(123, "", server);
        Assert.assertEquals(e.getErrorCode(), 123);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error 123, server " + server);

        e = new ProtonException(111, "Some error", server);
        Assert.assertEquals(e.getErrorCode(), 111);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Some error, server " + server);
    }

    @Test(groups = { "unit" })
    public void testHandleException() {
        ProtonNode server = ProtonNode.builder().build();
        Throwable cause = new RuntimeException();
        ProtonException e = ProtonException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), ProtonException.ERROR_UNKNOWN);
        Assert.assertEquals(e.getCause(), cause);
        Assert.assertEquals(e.getMessage(),
                "Unknown error " + ProtonException.ERROR_UNKNOWN + ", server " + server);

        e = ProtonException.of("Some error", server);
        Assert.assertEquals(e.getErrorCode(), ProtonException.ERROR_UNKNOWN);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Some error, server " + server);

        Assert.assertEquals(e, ProtonException.of(e, server));

        cause = new ExecutionException(null);
        e = ProtonException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), ProtonException.ERROR_UNKNOWN);
        Assert.assertEquals(e.getCause(), cause);
        Assert.assertEquals(e.getMessage(),
                "Unknown error " + ProtonException.ERROR_UNKNOWN + ", server " + server);

        e = ProtonException.of((ExecutionException) null, server);
        Assert.assertEquals(e.getErrorCode(), ProtonException.ERROR_UNKNOWN);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(),
                "Unknown error " + ProtonException.ERROR_UNKNOWN + ", server " + server);

        cause = new ExecutionException(new ProtonException(-100, (Throwable) null, server));
        e = ProtonException.of(cause, server);
        Assert.assertEquals(e, cause.getCause());
        Assert.assertEquals(e.getErrorCode(), -100);
        Assert.assertNull(e.getCause());
        Assert.assertEquals(e.getMessage(), "Unknown error -100, server " + server);

        cause = new ExecutionException(new IllegalArgumentException());
        e = ProtonException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), ProtonException.ERROR_UNKNOWN);
        Assert.assertEquals(e.getCause(), cause.getCause());
        Assert.assertEquals(e.getMessage(),
                "Unknown error " + ProtonException.ERROR_UNKNOWN + ", server " + server);

        cause = new ExecutionException(new IllegalArgumentException("Code: 12345. Something goes wrong..."));
        e = ProtonException.of(cause, server);
        Assert.assertEquals(e.getErrorCode(), 12345);
        Assert.assertEquals(e.getCause(), cause.getCause());
        Assert.assertEquals(e.getMessage(), cause.getCause().getMessage() + ", server " + server);
    }
}
