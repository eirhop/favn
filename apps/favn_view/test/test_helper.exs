ExUnit.start(capture_log: true)

Application.put_env(:favn_view, :active_workspace_configuration_fun, fn operator_context ->
  {:ok,
   FavnOrchestrator.WorkspaceConfiguration.without_active_deployment(
     operator_context.workspace_id
   )}
end)
