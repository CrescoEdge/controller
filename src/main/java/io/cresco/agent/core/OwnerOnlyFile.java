package io.cresco.agent.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

/**
 * Reads a secret from a file only when no one but the agent's user can read or replace it: a
 * regular file (not a symlink) owned by the JVM user with mode 0600 or 0400, in a directory owned
 * by that user or root that is not group- or world-writable. Used for db_key_file (OUT-03a) and for
 * the {@code <param>_file} secret sources (OUT-03b). Fails closed: on a filesystem without POSIX
 * owner/mode the file cannot be checked and is refused.
 */
public final class OwnerOnlyFile {

    /** The file failed a check or could not be read. The message names the file, never its content. */
    public static final class UnsafeFileException extends IllegalStateException {
        UnsafeFileException(String msg) { super(msg); }
        UnsafeFileException(String msg, Throwable cause) { super(msg, cause); }
    }

    private static final Set<PosixFilePermission> OWNER_ONLY =
            EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);

    private OwnerOnlyFile() {}

    /** The file's content with surrounding whitespace (a trailing newline) stripped; {@code label} names it in errors. */
    public static String read(Path file, String label) {
        if (file == null) throw new UnsafeFileException(label + ": no path");
        Path p = file.toAbsolutePath().normalize();
        if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
            throw new UnsafeFileException(label + " " + p + " does not exist");
        }
        if (Files.isSymbolicLink(p)) {
            throw new UnsafeFileException(label + " " + p + " is a symbolic link; point " + label + " at the file itself");
        }
        if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
            throw new UnsafeFileException(label + " " + p + " is not a regular file");
        }
        String me = System.getProperty("user.name");
        try {
            PosixFileAttributes a = Files.readAttributes(p, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (me == null || !me.equals(a.owner().getName())) {
                throw new UnsafeFileException(label + " " + p + " is owned by " + a.owner().getName() + ", not by the agent user " + me);
            }
            Set<PosixFilePermission> perms = a.permissions();
            if (!OWNER_ONLY.containsAll(perms) || !perms.contains(PosixFilePermission.OWNER_READ)) {
                throw new UnsafeFileException(label + " " + p + " has mode " + PosixFilePermissions.toString(perms)
                        + "; it must be 0600 or 0400 (owner read/write only)");
            }
            Path dir = p.getParent().toRealPath();
            PosixFileAttributes d = Files.readAttributes(dir, PosixFileAttributes.class);
            String dirOwner = d.owner().getName();
            if (!me.equals(dirOwner) && !"root".equals(dirOwner)) {
                throw new UnsafeFileException(label + " directory " + dir + " is owned by " + dirOwner + "; it must be owned by " + me + " or root");
            }
            if (d.permissions().contains(PosixFilePermission.GROUP_WRITE) || d.permissions().contains(PosixFilePermission.OTHERS_WRITE)) {
                throw new UnsafeFileException(label + " directory " + dir + " is group- or world-writable; the file could be replaced");
            }
        } catch (UnsupportedOperationException uoe) {
            throw new UnsafeFileException(label + " " + p + ": this filesystem has no POSIX owner/mode, so the file cannot be checked");
        } catch (IOException ioe) {
            throw new UnsafeFileException(label + " " + p + ": " + ioe.getMessage(), ioe);
        }
        try {
            return new String(Files.readAllBytes(p), StandardCharsets.UTF_8).strip();
        } catch (IOException ioe) {
            throw new UnsafeFileException(label + " " + p + " unreadable: " + ioe.getMessage(), ioe);
        }
    }
}
