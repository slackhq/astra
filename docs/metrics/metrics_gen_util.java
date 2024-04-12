import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class Scratch {
  public static void main(String[] args) throws IOException {
    class Metric {
      public String name = "";
      public String description = "";
      public String type = "";
      public final Map<String, Set<String>> tags = new HashMap<>();
    }

    Path path = Paths.get("metrics.txt");

    List<Metric> results = new ArrayList<>();
    Stream<String> lines = Files.lines(path);

    AtomicReference<Metric> workingMetric = new AtomicReference<>();

    final String HELP = "# HELP ";
    final String TYPE = "# TYPE ";
    lines.forEach(line -> {
      if (line.startsWith(HELP)) {
        if (workingMetric.get() != null) {
          results.removeIf(metric -> Objects.equals(metric.name, workingMetric.get().name));
          results.add(workingMetric.get());
        }

        int secondSpace = line.indexOf(" ", HELP.length());
        String name;
        String description = "";
        if (secondSpace > -1) {
          name = line.substring(HELP.length(), secondSpace);
          description = line.substring(secondSpace + 1);
        } else {
          name = line.substring(HELP.length());
        }

        String finalName = name;
        Optional<Metric> existing = results.stream().filter(metric -> Objects.equals(metric.name, finalName)).findFirst();

        if (existing.isPresent()) {
          workingMetric.set(existing.get());
        } else {
          workingMetric.set(new Metric());
          workingMetric.get().name = name;
          workingMetric.get().description = description;
        }
      } else if (line.startsWith(TYPE)) {
        workingMetric.get().type = line.split(" ")[3];
      } else {
        for (String tag : line.substring(line.indexOf("{") + 1, line.indexOf("}")).split(",")) {
          String tagName = tag.split("=")[0];
          String tagValue = tag.split("=")[1].replaceAll("\"", "");

          if (workingMetric.get().tags.containsKey(tagName)) {
            workingMetric.get().tags.get(tagName).add(tagValue);
          } else {
            Set<String> tagValues = new HashSet<>();
            tagValues.add(tagValue);
            workingMetric.get().tags.put(tagName, tagValues);
          }
        }
      }
    });

    StringBuilder stringBuilder = new StringBuilder();
    String tagsTemplate = "<def title=\"$tag_keys\"></def>";
    String template = """
          <def title="$title | $type">
            $description
            <deflist type="full" collapsible="true">
              <def title="labels" default-state="collapsed">
                <deflist type="full">
                  $tags<include from="Metrics-reference.md" element-id="common-configs" />
                </deflist>
              </def>
            </deflist>
          </def>
        """;

    results.stream().filter((metric) -> {
          return !metric.name.startsWith("kafka") &&
              !metric.name.startsWith("jvm") &&
              !metric.name.startsWith("grpc") &&
              !metric.name.startsWith("system") &&
              !metric.name.startsWith("process") &&
              !metric.name.startsWith("armeria");
        }).sorted(Comparator.comparing(o -> o.name))
        .forEach(metric -> {
          StringBuilder tagsString = new StringBuilder();
          metric.tags.forEach((key, tagValues) -> {
            if (!key.startsWith("astra_")) {
              tagsString.append(tagsTemplate.replace("$tag_keys", key)
                  .replace("$tag_values", String.join(", ", tagValues)));
            }
          });
          String tString = tagsString.toString();
          stringBuilder.append(template
              .replace("$title", metric.name)
              .replace("$description\n", metric.description.isEmpty() ? "" : metric.description + "\n")
              .replace("$type", metric.type)
              .replace("$tags", tString.isEmpty() ? "" : tString + "\n"));
        });

    Path out = Paths.get("metrics-out.txt");
    Files.writeString(out, stringBuilder.toString());
  }
}
