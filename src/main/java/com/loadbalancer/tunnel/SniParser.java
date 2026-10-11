package com.loadbalancer.tunnel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Lightweight, non-blocking byte inspector that extracts the Server Name Indication (SNI)
 * hostname from a TLS {@code ClientHello} record without decrypting or terminating TLS.
 *
 * <p>Implements specification Section 3.F:
 * <i>"The SNI hostname is parsed from the ClientHello for logging and metrics only.
 * There is no hostname-to-pool mapping. TLS is never terminated."</i></p>
 *
 * <h3>RFC 6066 &amp; RFC 5246 TLS Handshake Anatomy</h3>
 * <pre>
 * [0]     ContentType = 0x16 (Handshake)
 * [1..2]  TLS Version (0x03, 0x01..0x03)
 * [3..4]  Record Length
 * [5]     Handshake Type = 0x01 (ClientHello)
 * [6..8]  Handshake Length
 * [9..10] Client Version
 * [11..42] Random (32 bytes)
 * [43]    Session ID length (L1) -> skip L1 bytes
 * [..]    Cipher Suites length (L2, 2 bytes) -> skip L2 bytes
 * [..]    Compression Methods length (L3, 1 byte) -> skip L3 bytes
 * [..]    Extensions length (L4, 2 bytes)
 * [..]    Extension Type (0x0000 = server_name), Extension Length
 * [..]    Server Name List length (2 bytes), Name Type (0x00 = host_name), Host Name length (2 bytes)
 * [..]    Host Name (UTF-8 string)
 * </pre>
 */
public final class SniParser {

    private static final Logger log = LoggerFactory.getLogger(SniParser.class);

    private static final byte CONTENT_TYPE_HANDSHAKE = 0x16;
    private static final byte HANDSHAKE_TYPE_CLIENT_HELLO = 0x01;
    private static final int EXTENSION_SERVER_NAME = 0x0000;
    private static final byte NAME_TYPE_HOST_NAME = 0x00;

    private SniParser() {}

    /**
     * Parses the SNI hostname from initial handshake bytes if present.
     *
     * @param bytes the initial segment of bytes received from the client
     * @return {@link Optional} containing the SNI hostname, or empty if not TLS or SNI is absent
     */
    public static Optional<String> parseSni(byte[] bytes) {
        if (bytes == null || bytes.length < 44) {
            return Optional.empty();
        }

        try {
            int offset = 0;

            // 1. TLS Record Header: Content Type must be 0x16 (Handshake)
            if (bytes[offset++] != CONTENT_TYPE_HANDSHAKE) {
                return Optional.empty();
            }

            // Skip TLS version (2 bytes) and record length (2 bytes)
            offset += 4;
            if (offset >= bytes.length) return Optional.empty();

            // 2. Handshake Type: must be 0x01 (ClientHello)
            if (bytes[offset++] != HANDSHAKE_TYPE_CLIENT_HELLO) {
                return Optional.empty();
            }

            // Skip Handshake length (3 bytes) and Client Version (2 bytes)
            offset += 5;
            // Skip Random (32 bytes)
            offset += 32;
            if (offset >= bytes.length) return Optional.empty();

            // 3. Skip Session ID
            int sessionIdLength = bytes[offset++] & 0xFF;
            offset += sessionIdLength;
            if (offset + 2 > bytes.length) return Optional.empty();

            // 4. Skip Cipher Suites (2-byte length prefix)
            int cipherSuitesLength = readUint16(bytes, offset);
            offset += 2 + cipherSuitesLength;
            if (offset >= bytes.length) return Optional.empty();

            // 5. Skip Compression Methods (1-byte length prefix)
            int compressionMethodsLength = bytes[offset++] & 0xFF;
            offset += compressionMethodsLength;
            if (offset + 2 > bytes.length) return Optional.empty();

            // 6. Extensions length (2 bytes)
            int extensionsLength = readUint16(bytes, offset);
            offset += 2;
            int extensionsEnd = Math.min(offset + extensionsLength, bytes.length);

            // 7. Iterate through extensions to find server_name (0x0000)
            while (offset + 4 <= extensionsEnd) {
                int extType = readUint16(bytes, offset);
                int extLen = readUint16(bytes, offset + 2);
                offset += 4;

                if (extType == EXTENSION_SERVER_NAME) {
                    if (offset + 2 > extensionsEnd) return Optional.empty();
                    // Server Name List Length (2 bytes)
                    offset += 2;
                    if (offset + 3 > extensionsEnd) return Optional.empty();

                    int nameType = bytes[offset++] & 0xFF;
                    int hostNameLen = readUint16(bytes, offset);
                    offset += 2;

                    if (nameType == NAME_TYPE_HOST_NAME && offset + hostNameLen <= bytes.length) {
                        String hostname = new String(bytes, offset, hostNameLen, StandardCharsets.UTF_8);
                        log.debug("Extracted SNI hostname from ClientHello: {}", hostname);
                        return Optional.of(hostname);
                    }
                    return Optional.empty();
                } else {
                    offset += extLen;
                }
            }
        } catch (Exception e) {
            log.trace("Failed to parse SNI from packet (likely not TLS or malformed): {}", e.getMessage());
        }

        return Optional.empty();
    }

    private static int readUint16(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
    }
}
