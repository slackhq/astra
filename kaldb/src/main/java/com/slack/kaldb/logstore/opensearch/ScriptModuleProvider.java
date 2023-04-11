package com.slack.kaldb.logstore.opensearch;

import java.nio.file.Path;
import java.util.List;
import org.opensearch.common.settings.Settings;
import org.opensearch.painless.PainlessPlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.script.ScriptModule;

/**
 * The ScriptModule object appears to only be able to be instantiated once safely. This class makes
 * it a singleton, while attempting to avoid needing to pass parameters. This may eventually be
 * folded into the OpenSearchAdapter class.
 */
public class ScriptModuleProvider {
  private static ScriptModule scriptModule = null;

  public static ScriptModule getInstance() {
    if (scriptModule == null) {
      PluginsService pluginsService =
          new PluginsService(
              Settings.builder().build(), Path.of(""), null, null, List.of(PainlessPlugin.class));
      scriptModule =
          new ScriptModule(
              pluginsService.updatedSettings(), pluginsService.filterPlugins(ScriptPlugin.class));
    }
    return scriptModule;
  }
}
