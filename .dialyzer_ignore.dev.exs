{common, _binding} = Code.eval_file(".dialyzer_ignore.exs", __DIR__)

common ++
  [
    {"lib/favn_view/router.ex", "Function PhoenixStorybook.Mount.on_mount/4 does not exist."},
    {"lib/favn_view/router.ex",
     "Function PhoenixStorybook.AssetNotFoundController.init/1 does not exist."},
    {"lib/favn_view/router.ex", "Function PhoenixStorybook.JSAssets.init/1 does not exist."},
    {"lib/favn_view/router.ex",
     "Function PhoenixStorybook.Story.ComponentIframeLive.__live__/0 does not exist."},
    {"lib/favn_view/router.ex", "Function PhoenixStorybook.StoryLive.__live__/0 does not exist."},
    {"lib/favn_view/router.ex",
     "Function PhoenixStorybook.VisualTestLive.__live__/0 does not exist."},
    {"lib/favn_view/storybook.ex",
     "Function PhoenixStorybook.ExsCompiler.compile_exs/3 does not exist."},
    {"lib/favn_view/storybook.ex",
     "Function PhoenixStorybook.Stories.StoryValidator.validate/1 does not exist."}
  ]
