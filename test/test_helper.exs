trace? = "--trace" in System.argv()

ExUnit.start(capture_log: !trace?)
Logger.configure(level: if(trace?, do: :debug, else: :warning))
