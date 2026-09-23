/*
 * ABI subset of DuckDB's public C header from v1.5.5 (MIT license).
 * Source: https://github.com/duckdb/duckdb/blob/v1.5.5/src/include/duckdb.h
 * Keep in step with the required-symbol checks and v1.5.2/v1.5.5 tests.
 */
#ifndef FAVN_SEMANTIC_DUCKDB_H
#define FAVN_SEMANTIC_DUCKDB_H
#include <stdint.h>

typedef uint64_t duckdb_idx_t;
typedef void *duckdb_database;
typedef void *duckdb_connection;
typedef void *duckdb_config;
typedef struct {
    duckdb_idx_t columns;
    duckdb_idx_t rows;
    duckdb_idx_t changed;
    void *column_data;
    char *error;
    void *internal;
} duckdb_result;

typedef struct {
    const char *(*library_version)(void);
    int (*create_config)(duckdb_config *);
    int (*set_config)(duckdb_config, const char *, const char *);
    void (*destroy_config)(duckdb_config *);
    int (*open_ext)(const char *, duckdb_database *, duckdb_config, char **);
    void (*close)(duckdb_database *);
    int (*connect)(duckdb_database, duckdb_connection *);
    void (*disconnect)(duckdb_connection *);
    int (*query)(duckdb_connection, const char *, duckdb_result *);
    duckdb_idx_t (*row_count)(duckdb_result *);
    char *(*value_varchar)(duckdb_result *, duckdb_idx_t, duckdb_idx_t);
    void (*destroy_result)(duckdb_result *);
    void (*free_value)(void *);
} duckdb_api;
#endif
