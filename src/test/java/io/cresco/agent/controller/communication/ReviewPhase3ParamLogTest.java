package io.cresco.agent.controller.communication;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Independent adversarial review of Phase 3 (controller 0ff640d), 2026-11-04: "Never log message parameter values". A
 * failure is a finding. Review-only: never pushed.
 *
 * <p>Scans every logger statement of the controller's main sources (statements joined across lines, comments skipped) for
 * a message's parameter VALUES: {@code getParams()} (other than its key set), {@code gson.toJson} of a message, and a
 * pipeline node's {@code params} map (user-supplied plugin configuration from gpipelinesubmit).
 */
class ReviewPhase3ParamLogTest {

    private static final Pattern LOG_START = Pattern.compile("\\blog(ger)?\\.(trace|debug|info|warn|error)\\(");
    private static final Pattern[] VALUES = {
            Pattern.compile("\\.getParams\\(\\)(?!\\s*\\.\\s*(keySet|size|containsKey|isEmpty))"),
            Pattern.compile("gson\\.toJson\\(\\s*(me|ce|msg|rm|incoming|le|re)\\s*\\)"),
            Pattern.compile("\\b(gnode|node)\\.params\\b"),
    };

    @Test
    void noLoggerStatementCarriesParameterValues() throws Exception {
        Path root = Paths.get("src/main/java");
        assertTrue(Files.isDirectory(root), "run from the controller module");
        List<String> hits = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                List<String> lines = Files.readAllLines(f);
                boolean inBlock = false;
                for (int i = 0; i < lines.size(); i++) {
                    String l = lines.get(i);
                    String t = l.trim();
                    if (inBlock) { if (t.contains("*/")) inBlock = false; continue; }
                    if (t.startsWith("/*")) { if (!t.contains("*/")) inBlock = true; continue; }
                    if (t.startsWith("//") || t.startsWith("*")) continue;
                    if (!LOG_START.matcher(l).find()) continue;
                    StringBuilder stmt = new StringBuilder(l);
                    int j = i;
                    while (!stmt.toString().trim().endsWith(";") && j + 1 < lines.size() && j - i < 6) stmt.append(' ').append(lines.get(++j).trim());
                    String s = stmt.toString();
                    if (s.contains("ParamLog")) continue;
                    for (Pattern p : VALUES) {
                        if (p.matcher(s).find()) {
                            hits.add(root.relativize(f) + ":" + (i + 1) + ": " + s.trim());
                            break;
                        }
                    }
                }
            }
        }
        assertTrue(hits.isEmpty(), "logger statements that still log message parameter values:\n  " + String.join("\n  ", hits));
    }
}
