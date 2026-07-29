{common, _binding} = Code.eval_file(".dialyzer_ignore.exs", __DIR__)

common ++
  [
    {"test/support/conn_case.ex", "Function ExUnit.Callbacks.__merge__/4 does not exist."},
    {"test/support/conn_case.ex", "Function ExUnit.Callbacks.__noop__/0 does not exist."},
    {"test/support/conn_case.ex", "Function ExUnit.CaseTemplate.__proxy__/2 does not exist."}
  ]
