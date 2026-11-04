package io.cresco.agent.controller.agentcontroller;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Pattern;

/**
 * Plugin ids. An id names a directory ({@code plugin-data/<id>}), a registry row and a configuration, and arrives from
 * callers ({@code pluginadd}'s {@code inode_id}, {@code pluginremove}'s {@code pluginid}, the global's scheduling): every
 * one is checked against {@link #GRAMMAR} where it enters (review F6: {@code ../../victim} purged a directory outside
 * {@code plugin-data}). A purge deletes only a real directory strictly inside {@code plugin-data}, never through a link.
 */
public final class PluginIds {

    private PluginIds() { }

    /** 1-64 of {@code A-Z a-z 0-9 . _ -}, starting with a letter or digit: one plain path segment, never {@code .} or {@code ..}. */
    public static final Pattern GRAMMAR = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");

    /** Is {@code id} a valid plugin id? */
    public static boolean isValid(String id) {
        return id != null && GRAMMAR.matcher(id).matches() && !id.contains("..");
    }

    /** Why {@code id} is not a valid plugin id, or null. */
    public static String refusal(String id) {
        if (id == null) return "no plugin id";
        return isValid(id) ? null : "invalid plugin id '" + printable(id) + "' (1-64 of A-Z a-z 0-9 . _ -, starting with a letter or digit, no '..')";
    }

    private static String printable(String s) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < s.length() && i < 80; i++) {
            char c = s.charAt(i);
            b.append(c >= 0x20 && c < 0x7f ? c : '?');
        }
        return b.toString();
    }

    /**
     * Delete the data directory of plugin {@code id} below {@code pluginDataBase}: only for a valid id, only when the
     * directory is a real directory (not a link) whose real parent is the real {@code pluginDataBase}, and without following
     * any link inside it (a link is removed, never its target).
     *
     * @return null when deleted (or there was nothing to delete), else why nothing was deleted
     */
    public static String purge(Path pluginDataBase, String id) throws IOException {
        String why = refusal(id);
        if (why != null) return why;
        if (pluginDataBase == null || !Files.isDirectory(pluginDataBase, LinkOption.NOFOLLOW_LINKS)) {
            return pluginDataBase + " is not a directory";
        }
        Path base = pluginDataBase.toRealPath();
        Path target = base.resolve(id).normalize();
        if (!base.equals(target.getParent())) return "plugin id '" + id + "' is not one directory inside " + base;
        if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) return null;
        if (Files.isSymbolicLink(target) || !Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
            return target + " is not a real directory (a link or a file): not purged";
        }
        if (!base.equals(target.toRealPath().getParent())) return target + " does not resolve inside " + base + ": not purged";
        deleteTree(target);
        return null;
    }

    /** Delete {@code dir} and everything below it; links are deleted themselves, never followed. */
    static void deleteTree(Path dir) throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);                              // a link (attrs of the link itself) or a file
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                if (exc != null) throw exc;
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
