#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct { uint64_t a, b, c; void *d, *e, *f; } result;

static void block(void) {
    if (getenv("FAVN_SEMANTIC_FIXTURE_IGNORE_TERM")) signal(SIGTERM, SIG_IGN);
    const char *marker = getenv("FAVN_SEMANTIC_FIXTURE_MARKER");
    if (marker) {
        char *temporary = malloc(strlen(marker) + 5);
        if (temporary) {
            sprintf(temporary, "%s.tmp", marker);
            int fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (fd >= 0) {
                char pid[32];
                int size = snprintf(pid, sizeof(pid), "%ld", (long)getpid());
                int written = size > 0 && write(fd, pid, (size_t)size) == size;
                int closed = close(fd) == 0;
                if (written && closed) rename(temporary, marker);
            }
            free(temporary);
        }
    }
    for (;;) pause();
}

__attribute__((constructor)) static void initialize(void) {
    const char *mode = getenv("FAVN_SEMANTIC_FIXTURE_MODE");
    if (mode && strcmp(mode, "constructor") == 0) block();
}

const char *duckdb_library_version(void) {
#ifdef FAVN_SEMANTIC_TEST_UNSUPPORTED_VERSION
    return "v9.9.9";
#else
    return "v1.5.5";
#endif
}
int duckdb_create_config(void **value) { *value = (void *)1; return 0; }
int duckdb_set_config(void *value, const char *key, const char *option) {
    (void)value; (void)key; (void)option; return 0;
}
void duckdb_destroy_config(void **value) { *value = NULL; }
int duckdb_open_ext(const char *path, void **database, void *config, char **error) {
    (void)path; (void)config; (void)error; *database = (void *)1; return 0;
}
void duckdb_close(void **database) { *database = NULL; }
int duckdb_connect(void *database, void **connection) {
    (void)database; *connection = (void *)1; return 0;
}
void duckdb_disconnect(void **connection) { *connection = NULL; }
int duckdb_query(void *connection, const char *sql, result *output) {
    (void)connection; (void)sql; (void)output; block(); return 1;
}
uint64_t duckdb_row_count(result *output) { (void)output; return 1; }
char *duckdb_value_varchar(result *output, uint64_t column, uint64_t row) {
    (void)output; (void)column; (void)row; return NULL;
}
void duckdb_destroy_result(result *output) { (void)output; }
#ifndef FAVN_SEMANTIC_TEST_MISSING_SYMBOL
void duckdb_free(void *value) { (void)value; }
#endif
