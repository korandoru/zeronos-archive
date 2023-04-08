package io.korandoru.zeronos.server.exception;

public sealed abstract class ZeronosServerException extends RuntimeException {

    public static final class FatalError extends ZeronosServerException {
    }

    public static final class RevisionNotFound extends ZeronosServerException {
    }

}
