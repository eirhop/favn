Code.require_file("umbrella_test_runner.ex", __DIR__)

System.argv()
|> Favn.UmbrellaTestRunner.run()
|> System.halt()
