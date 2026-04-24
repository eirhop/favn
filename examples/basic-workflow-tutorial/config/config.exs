import Config

config :favn,
  asset_modules: [
    FavnReferenceWorkload.Warehouse.Sources.CountryRegions,
    FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog,
    FavnReferenceWorkload.Warehouse.Raw.Customers,
    FavnReferenceWorkload.Warehouse.Raw.Products,
    FavnReferenceWorkload.Warehouse.Raw.Orders,
    FavnReferenceWorkload.Warehouse.Raw.OrderItems,
    FavnReferenceWorkload.Warehouse.Raw.Payments,
    FavnReferenceWorkload.Warehouse.Stg.Customers,
    FavnReferenceWorkload.Warehouse.Stg.OrderFacts,
    FavnReferenceWorkload.Warehouse.Stg.ProductDaily,
    FavnReferenceWorkload.Warehouse.Gold.Customer360,
    FavnReferenceWorkload.Warehouse.Gold.RevenueDaily,
    FavnReferenceWorkload.Warehouse.Gold.ChannelEfficiency,
    FavnReferenceWorkload.Warehouse.Gold.ExecutiveOverview,
    FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete
  ],
  pipeline_modules: [
    FavnReferenceWorkload.Pipelines.ReferenceWorkloadDaily
  ],
  connection_modules: [
    FavnReferenceWorkload.Connections.Warehouse
  ],
  connections: [
    warehouse: [database: ".favn/data/reference_workload.duckdb"]
  ],
  local: [
    storage: :memory
  ],
  runner_plugins: [
    {FavnDuckdb, execution_mode: :in_process}
  ]
