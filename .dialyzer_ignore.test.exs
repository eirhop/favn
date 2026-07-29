{common, _binding} = Code.eval_file(".dialyzer_ignore.exs", __DIR__)

common ++
  [
    ~r/^storybook\/.*:callback_info_missing Callback info about the PhoenixStorybook\./,
    ~r/^storybook\/.*:unknown_function Function PhoenixStorybook\./,
    {"storybook/components/selected_window_actions.story.exs",
     "The pattern can never match the type :error."},
    {"storybook/components/selected_window_actions.story.exs",
     "The pattern can never match the type :error | :success."},
    {"test/support/conn_case.ex", "Function ExUnit.Callbacks.__merge__/4 does not exist."},
    {"test/support/conn_case.ex", "Function ExUnit.Callbacks.__noop__/0 does not exist."},
    {"test/support/conn_case.ex", "Function ExUnit.CaseTemplate.__proxy__/2 does not exist."}
  ]
