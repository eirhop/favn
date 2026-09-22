/* One bounded request. The supervisor never loads DuckDB; only its child does. */
#include "semantic_duckdb.h"
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif
#ifdef __APPLE__
#include <sys/event.h>
#endif

#define REQUEST_LIMIT 262144U
#define AST_LIMIT 2000000U
#define FINAL_LIMIT 65536U
#define STARTUP_MS 10000
#define EXPRESSION_MS 5000
#define TERM_MS 2000
#define KILL_MS 3000

typedef struct { char *driver; char *parse; char *bind; } request;

static int64_t now_ms(void) {
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    return (int64_t)time.tv_sec * 1000 + time.tv_nsec / 1000000;
}

static uint32_t from_be(const unsigned char *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static void to_be(unsigned char *bytes, uint32_t value) {
    bytes[0] = (unsigned char)(value >> 24);
    bytes[1] = (unsigned char)(value >> 16);
    bytes[2] = (unsigned char)(value >> 8);
    bytes[3] = (unsigned char)value;
}

static int nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* -2: caller gone, -3: deadline, -4: out-of-order caller input. */
static int await_fd(int fd, short events, int64_t deadline, int watch_owner) {
    for (;;) {
        int64_t remaining = deadline - now_ms();
        if (remaining <= 0) return -3;
        struct pollfd fds[2] = {{.fd = fd, .events = events}, {.fd = 0, .events = POLLIN | POLLHUP}};
        int count = watch_owner && fd != 0 ? 2 : 1;
        int result = poll(fds, (nfds_t)count, (int)remaining);
        if (result < 0 && errno == EINTR) continue;
        if (result < 0) return -1;
        if (result == 0) return -3;
        if (count == 2 && (fds[1].revents & (POLLHUP | POLLERR | POLLNVAL))) return -2;
        if (count == 2 && (fds[1].revents & POLLIN)) return -4;
        if (fds[0].revents & (POLLERR | POLLNVAL)) return -1;
        if (fds[0].revents & (events | POLLHUP)) return 0;
    }
}

static int read_exact(int fd, void *buffer, size_t length, int64_t deadline, int watch_owner) {
    size_t requested = length;
    unsigned char *cursor = buffer;
    while (length) {
        int ready = await_fd(fd, POLLIN, deadline, watch_owner);
        if (ready) return ready;
        ssize_t got = read(fd, cursor, length);
        if (got < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        if (got <= 0) return fd == 0 ? -2 : length == requested ? -6 : -1;
        cursor += got;
        length -= (size_t)got;
    }
    return 0;
}

static int write_exact(int fd, const void *buffer, size_t length, int64_t deadline, int watch_owner) {
    const unsigned char *cursor = buffer;
    while (length) {
        int ready = await_fd(fd, POLLOUT, deadline, watch_owner);
        if (ready) return ready;
        ssize_t sent = write(fd, cursor, length);
        if (sent < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        if (sent <= 0) return -2;
        cursor += sent;
        length -= (size_t)sent;
    }
    return 0;
}

static int frame_write(int fd, unsigned char tag, const void *data, uint32_t length,
                       int64_t deadline, int watch_owner) {
    unsigned char header[5];
    to_be(header, length + 1);
    header[4] = tag;
    int result = write_exact(fd, header, sizeof(header), deadline, watch_owner);
    return result ? result : write_exact(fd, data, length, deadline, watch_owner);
}

static int frame_read(int fd, unsigned char *tag, unsigned char **data, uint32_t *length,
                      uint32_t limit, int64_t deadline, int watch_owner) {
    unsigned char header[4];
    int result = read_exact(fd, header, sizeof(header), deadline, watch_owner);
    if (result) return result;
    uint32_t size = from_be(header);
    if (size == 0 || size - 1 > limit) return -1;
    unsigned char *body = malloc(size);
    if (!body) return -1;
    result = read_exact(fd, body, size, deadline, watch_owner);
    if (result) { free(body); return result; }
    *tag = body[0];
    *length = size - 1;
    *data = body + 1;
    return 0;
}

static void frame_free(unsigned char *data) { free(data - 1); }

static int read_field(char **field, uint32_t *total, int64_t deadline) {
    unsigned char bytes[4];
    int result = read_exact(0, bytes, 4, deadline, 0);
    if (result) return result;
    uint32_t length = from_be(bytes);
    if (*total > REQUEST_LIMIT - 4 || !length || length > REQUEST_LIMIT - *total - 4) return -1;
    *total += length + 4;
    char *value = malloc((size_t)length + 1);
    if (!value) return -1;
    result = read_exact(0, value, length, deadline, 0);
    if (result || memchr(value, 0, length)) { free(value); return result ? result : -1; }
    value[length] = 0;
    *field = value;
    return 0;
}

static void request_free(request *value) {
    free(value->driver); free(value->parse); free(value->bind);
}

#ifdef FAVN_SEMANTIC_TEST_FAULTS
static int test_fault(const char *name) {
    const char *fault = getenv("FAVN_SEMANTIC_TEST_FAULT");
    return fault && strcmp(fault, name) == 0;
}

static void test_signal(char signal_name) {
    const char *path = getenv("FAVN_SEMANTIC_TEST_SIGNAL_LOG");
    if (!path) return;
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd < 0) return;
    write(fd, &signal_name, 1);
    close(fd);
}
#endif

#ifdef __APPLE__
static void *watch_parent(void *argument) {
    int fd = (int)(intptr_t)argument;
#ifdef FAVN_SEMANTIC_TEST_FAULTS
    if (test_fault("watch_error")) {
        const char *marker = getenv("FAVN_SEMANTIC_FIXTURE_MARKER");
        while (marker && access(marker, F_OK) != 0) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 10000000};
            nanosleep(&delay, NULL);
        }
        close(fd);
    }
#endif
    struct kevent event;
    int result;
    do { result = kevent(fd, NULL, 0, &event, 1, NULL); }
    while (result < 0 && errno == EINTR);
    kill(getpid(), SIGKILL);
    _exit(127);
}

static int arm_parent(pid_t parent) {
#ifdef FAVN_SEMANTIC_TEST_FAULTS
    if (test_fault("arm")) return -1;
#endif
    if (parent <= 1 || getppid() != parent) return -1;
    int fd = kqueue();
    if (fd < 0) return -1;
    struct kevent event;
    EV_SET(&event, parent, EVFILT_PROC, EV_ADD | EV_ENABLE | EV_ONESHOT, NOTE_EXIT, 0, NULL);
    if (kevent(fd, &event, 1, NULL, 0, NULL) < 0 || getppid() != parent) {
        close(fd); return -1;
    }
    pthread_attr_t attributes;
    if (pthread_attr_init(&attributes)) { close(fd); return -1; }
    pthread_t thread;
    int result = pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_DETACHED);
    if (!result) result = pthread_create(&thread, &attributes, watch_parent, (void *)(intptr_t)fd);
    pthread_attr_destroy(&attributes);
    if (result) close(fd);
    return result ? -1 : 0;
}
#else
static int arm_parent(pid_t parent) {
#ifdef FAVN_SEMANTIC_TEST_FAULTS
    if (test_fault("arm")) return -1;
#endif
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0) return -1;
    return parent > 1 && getppid() == parent ? 0 : -1;
}
#endif

#define RESOLVE(name, symbol) do { *(void **)(&api->name) = dlsym(handle, "duckdb_" symbol); \
    if (!api->name) return -1; } while (0)

static int load_api(void *handle, duckdb_api *api) {
    RESOLVE(library_version, "library_version");
    RESOLVE(create_config, "create_config");
    RESOLVE(set_config, "set_config");
    RESOLVE(destroy_config, "destroy_config");
    RESOLVE(open_ext, "open_ext");
    RESOLVE(close, "close");
    RESOLVE(connect, "connect");
    RESOLVE(disconnect, "disconnect");
    RESOLVE(query, "query");
    RESOLVE(row_count, "row_count");
    RESOLVE(value_varchar, "value_varchar");
    RESOLVE(destroy_result, "destroy_result");
    RESOLVE(free_value, "free");
    return 0;
}

static int duckdb_open_private(duckdb_api *api, duckdb_database *database,
                               duckdb_connection *connection) {
    duckdb_config config = NULL;
    if (api->create_config(&config)) return -1;
    const char *keys[] = {"enable_external_access", "autoinstall_known_extensions",
                          "autoload_known_extensions", "threads", "memory_limit",
                          "max_expression_depth"};
    const char *values[] = {"false", "false", "false", "1", "128MB", "128"};
    int result = 0;
    for (size_t index = 0; index < 6; index++)
        if (api->set_config(config, keys[index], values[index])) result = -1;
    if (!result && api->open_ext(":memory:", database, config, NULL)) result = -1;
    api->destroy_config(&config);
    if (!result && api->connect(*database, connection)) result = -1;
    return result;
}

static int query_value(duckdb_api *api, duckdb_connection connection, const char *sql,
                       duckdb_idx_t column, char **value, uint32_t limit) {
    duckdb_result result = {0};
    int ok = api->query(connection, sql, &result) == 0 && api->row_count(&result) == 1;
    char *native = ok ? api->value_varchar(&result, column, 0) : NULL;
    if (native) {
        size_t size = strnlen(native, (size_t)limit + 1);
        if (size && size <= limit) {
            *value = malloc(size + 1);
            if (*value) memcpy(*value, native, size + 1);
        }
        api->free_value(native);
    }
    api->destroy_result(&result);
    return *value ? 0 : -1;
}

static int child_main(request *request_data, pid_t parent, int output, int approval) {
    if (setsid() < 0) return -1;
    close(0);
    int null_fd = open("/dev/null", O_WRONLY);
    if (null_fd < 0 || dup2(null_fd, 1) < 0 || dup2(null_fd, 2) < 0) return -1;
    if (null_fd > 2) close(null_fd);
    if (arm_parent(parent)) return frame_write(output, 'E', "o", 1, now_ms() + STARTUP_MS, 0);

    void *handle = dlopen(request_data->driver, RTLD_NOW | RTLD_LOCAL);
    duckdb_api api = {0};
    if (!handle || load_api(handle, &api)) return frame_write(output, 'E', "f", 1, now_ms() + STARTUP_MS, 0);
    const char *version = api.library_version();
#ifdef __APPLE__
    int supported = version && !strcmp(version, "v1.5.5");
#else
    int supported = version && (!strcmp(version, "v1.5.2") || !strcmp(version, "v1.5.5"));
#endif
    if (!supported) return frame_write(output, 'E', "u", 1, now_ms() + STARTUP_MS, 0);
    duckdb_database database = NULL;
    duckdb_connection connection = NULL;
    if (duckdb_open_private(&api, &database, &connection)) return frame_write(output, 'E', "f", 1, now_ms() + STARTUP_MS, 0);
    int result = frame_write(output, 'R', NULL, 0, now_ms() + STARTUP_MS, 0);
    char *ast = NULL;
    if (!result) result = query_value(&api, connection, request_data->parse, 0, &ast, AST_LIMIT);
    if (!result) result = frame_write(output, 'A', ast, (uint32_t)strlen(ast), now_ms() + EXPRESSION_MS, 0);
    free(ast);
    unsigned char decision = 0;
    if (!result && read(approval, &decision, 1) == 1 && decision == 'Y') {
#ifdef FAVN_SEMANTIC_TEST_FAULTS
        const char *bind_marker = getenv("FAVN_SEMANTIC_TEST_BIND_MARKER");
        if (bind_marker) {
            int fd = open(bind_marker, O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (fd >= 0) close(fd);
        }
#endif
        char *type = NULL;
        if (query_value(&api, connection, request_data->bind, 1, &type, FINAL_LIMIT - 64) == 0) {
            size_t version_size = strlen(version) + 1;
            size_t type_size = strlen(type);
            char *payload = malloc(version_size + type_size);
            if (payload) {
                memcpy(payload, version, version_size);
                memcpy(payload + version_size, type, type_size);
                result = frame_write(output, 'S', payload, (uint32_t)(version_size + type_size), now_ms() + EXPRESSION_MS, 0);
                free(payload);
            } else result = -1;
            free(type);
        } else result = frame_write(output, 'E', "b", 1, now_ms() + EXPRESSION_MS, 0);
    }
    api.disconnect(&connection);
    api.close(&database);
    dlclose(handle);
    return result;
}

static int wait_child(pid_t child, int64_t deadline, int *status, int *reaped) {
    while (now_ms() < deadline) {
        pid_t found = waitpid(child, status, WNOHANG);
        if (found == child) { *reaped = 1; return 0; }
        if (found < 0 && errno == EINTR) continue;
        if (found < 0) return -1;
        struct timespec delay = {.tv_sec = 0, .tv_nsec = 10000000};
        nanosleep(&delay, NULL);
    }
    return -3;
}

static int settle(pid_t child, int *status, int *reaped) {
    if (*reaped) return 0;
#ifdef FAVN_SEMANTIC_TEST_FAULTS
    if (test_fault("cleanup_unconfirmed")) return -1;
#endif
    pid_t found;
    do { found = waitpid(child, status, WNOHANG); }
    while (found < 0 && errno == EINTR);
    if (found == child) { *reaped = 1; return 0; }
    if (found < 0) return -1;
    int term_result = kill(child, SIGTERM);
#ifdef FAVN_SEMANTIC_TEST_FAULTS
    test_signal('T');
#endif
    if (term_result && errno != ESRCH) return -1;
    int waited = wait_child(child, now_ms() + TERM_MS, status, reaped);
    if (waited == 0) return 0;
    if (waited != -3) return -1;
    int kill_result = kill(child, SIGKILL);
#ifdef FAVN_SEMANTIC_TEST_FAULTS
    test_signal('K');
#endif
    if (kill_result && errno != ESRCH) return -1;
    return wait_child(child, now_ms() + KILL_MS, status, reaped);
}

static int supervisor(void) {
    signal(SIGPIPE, SIG_IGN);
    if (nonblocking(0) || nonblocking(1)) return 1;
    int64_t startup = now_ms() + STARTUP_MS;
    unsigned char version;
    if (read_exact(0, &version, 1, startup, 0) || version != 1) return 1;
    request data = {0};
    uint32_t total = 1;
    if (read_field(&data.driver, &total, startup) || read_field(&data.parse, &total, startup) ||
        read_field(&data.bind, &total, startup)) { request_free(&data); return 1; }
    int result_pipe[2], command_pipe[2];
    if (pipe(result_pipe) || pipe(command_pipe)) { request_free(&data); return 1; }
    pid_t parent = getpid();
    pid_t child = fork();
    if (child < 0) { request_free(&data); return 1; }
    if (child == 0) {
        close(result_pipe[0]); close(command_pipe[1]);
        int result = child_main(&data, parent, result_pipe[1], command_pipe[0]);
        _exit(result ? 1 : 0);
    }
    request_free(&data);
    close(result_pipe[1]); close(command_pipe[0]);
    nonblocking(result_pipe[0]); nonblocking(command_pipe[1]);
    unsigned char identity[8];
    to_be(identity, (uint32_t)parent); to_be(identity + 4, (uint32_t)child);
    int failure = frame_write(1, 'I', identity, 8, startup, 1);
    int64_t deadline = startup;
    int ready = 0, ast = 0, accepted = 0, rejected = 0, finished = 0, status = 0, reaped = 0;
    unsigned char final_tag = 'E', final_data = 'f';
    unsigned char *success = NULL;
    uint32_t success_size = 0;
    while (!failure && !finished) {
        unsigned char tag, *payload;
        uint32_t size;
        int read_result = frame_read(result_pipe[0], &tag, &payload, &size,
                                     ast ? FINAL_LIMIT : AST_LIMIT, deadline, 1);
        if (read_result == -6 && rejected) break;
        if (read_result) { failure = read_result; break; }
        if (tag == 'R' && !ready && size == 0) {
            ready = 1;
            deadline = now_ms() + EXPRESSION_MS;
            failure = frame_write(1, 'R', NULL, 0, deadline, 1);
        } else if (tag == 'A' && ready && !ast && size <= AST_LIMIT) {
            ast = 1;
            failure = frame_write(1, 'A', payload, size, deadline, 1);
            unsigned char decision;
            if (!failure) failure = read_exact(0, &decision, 1, deadline, 0);
            if (!failure && decision != 'Y' && decision != 'N') failure = -1;
            if (!failure) { accepted = decision == 'Y'; rejected = !accepted;
                failure = write_exact(command_pipe[1], &decision, 1, deadline, 1); }
        } else if (tag == 'S' && accepted && size > 2) {
            success = malloc(size);
            if (!success) failure = -1;
            else { memcpy(success, payload, size); success_size = size; final_tag = 'S'; finished = 1; }
        } else if (tag == 'E' && size == 1) {
            final_data = payload[0]; finished = 1;
        } else failure = -1;
        frame_free(payload);
    }
    close(command_pipe[1]);
    if (!failure && !finished && rejected) final_data = 'i';
    if (!failure && (finished || rejected)) {
        int waiting = wait_child(child, deadline, &status, &reaped);
        if (waiting) failure = waiting;
        else if (!WIFEXITED(status) || WEXITSTATUS(status)) failure = -1;
    }
    if (!failure && finished) {
        unsigned char trailing;
        int extra = read(result_pipe[0], &trailing, 1);
        if (extra != 0) failure = -1;
    }
    if (!reaped && settle(child, &status, &reaped)) failure = -5;
    close(result_pipe[0]);
    if (failure == -5 || !reaped) { free(success); frame_write(1, 'U', NULL, 0, now_ms() + 1000, 1); return 0; }
    if (failure) { final_tag = 'E'; final_data = failure == -3 ? 't' : 'f'; }
    if (rejected && !failure) { final_tag = 'E'; final_data = 'i'; }
    int sent = final_tag == 'S'
        ? frame_write(1, 'S', success, success_size, now_ms() + 1000, 1)
        : frame_write(1, 'E', &final_data, 1, now_ms() + 1000, 1);
    free(success);
    return sent ? 1 : 0;
}

int main(void) { return supervisor(); }
