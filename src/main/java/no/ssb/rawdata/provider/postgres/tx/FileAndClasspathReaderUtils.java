package no.ssb.rawdata.provider.postgres.tx;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileAndClasspathReaderUtils {

    public static Path currentPath() {
        return Paths.get(".").toAbsolutePath().normalize();
    }

    public static String readFileOrClasspathResource(String path) {
        String utf8Str;
        if (Files.exists(Paths.get(path))) {
            utf8Str = readFileAsUtf8(path);
        } else {
            utf8Str = getResourceAsString(path, StandardCharsets.UTF_8);
        }
        if (utf8Str == null) {
            utf8Str = getClassLoaderResourceAsString(path);
        }
        if (utf8Str == null) {
            throw new IllegalArgumentException("Resource not found: " + path);
        }
        return utf8Str;
    }

    public static String getClassLoaderResourceAsString(String path) {
        try {
            return new String(FileAndClasspathReaderUtils.class.getClassLoader().getResourceAsStream(path).readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getResourceAsString(String path, Charset charset) {
        try {
            URL systemResource = ClassLoader.getSystemResource(path);
            if (systemResource == null) {
                return null;
            }
            URLConnection conn = systemResource.openConnection();
            try (InputStream is = conn.getInputStream()) {
                byte[] bytes = is.readAllBytes();
                CharBuffer cbuf = CharBuffer.allocate(bytes.length);
                CoderResult coderResult = charset.newDecoder().decode(ByteBuffer.wrap(bytes), cbuf, true);
                if (coderResult.isError()) {
                    coderResult.throwException();
                }
                return cbuf.flip().toString();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String readFileAsUtf8(String pathStr) {
        try {
            Path path = Paths.get(pathStr);
            if (Files.notExists(path)) {
                throw new RuntimeException("File not found: " + pathStr);
            }
            byte[] bytes = Files.readAllBytes(path);
            CharBuffer cbuf = CharBuffer.allocate(bytes.length);
            CoderResult coderResult = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes), cbuf, true);
            if (coderResult.isError()) {
                coderResult.throwException();
            }
            return cbuf.flip().toString();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
