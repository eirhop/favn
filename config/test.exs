import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :favn_view, FavnView.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 0],
  secret_key_base: "/pvxXpMKDmziM4dr7zEcL3RwLUFIOjPJRAp/tAKL62VEd2+BZ1QE/4f8iUTMN9Bb",
  server: false

config :favn_view, dev_routes: false
