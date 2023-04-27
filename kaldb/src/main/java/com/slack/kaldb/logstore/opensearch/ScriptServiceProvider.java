package com.slack.kaldb.logstore.opensearch;

import java.nio.file.Path;
import java.util.List;
import org.opensearch.index.IndexSettings;
import org.opensearch.painless.PainlessPlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;

/**
 * The ScriptModule object appears to only be able to be instantiated once safely. This class makes
 * it a singleton, while attempting to avoid needing to pass parameters. This may eventually be
 * folded into the OpenSearchAdapter class.
 */
public class ScriptServiceProvider {
  private static ScriptService scriptService = null;

  public static ScriptService getInstance() {
    if (scriptService == null) {
      IndexSettings indexSettings = OpenSearchAdapter.buildIndexSettings();
      PluginsService pluginsService =
          new PluginsService(
              indexSettings.getSettings(), Path.of(""), null, null, List.of(PainlessPlugin.class));
      ScriptModule scriptModule =
          new ScriptModule(
              pluginsService.updatedSettings(), pluginsService.filterPlugins(ScriptPlugin.class));

      scriptService =
          new ScriptService(
              indexSettings.getSettings(), scriptModule.engines, scriptModule.contexts);
      return scriptService;
    }
    return scriptService;
  }
}
