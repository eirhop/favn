#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static void write_all(const void *data, size_t size, int slow) {
    const unsigned char *bytes = data;
    for (size_t index = 0; index < size; index++) {
        if (write(1, bytes + index, 1) != 1) _exit(2);
        if (slow) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 2000000};
            nanosleep(&delay, NULL);
        }
    }
}

static void frame(unsigned char tag, const void *payload, uint32_t size, int slow) {
    unsigned char header[5] = {(unsigned char)((size + 1) >> 24),
                               (unsigned char)((size + 1) >> 16),
                               (unsigned char)((size + 1) >> 8),
                               (unsigned char)(size + 1), tag};
    write_all(header, sizeof(header), slow);
    write_all(payload, size, slow);
}

int main(void) {
    const char *mode = getenv("FAVN_SEMANTIC_TEST_PROTOCOL");
    if (!mode) return 2;
    if (strcmp(mode, "oversized") == 0) {
        unsigned char length[4] = {0, 30, 132, 130};
        write_all(length, sizeof(length), 0);
        return 0;
    }

    uint32_t pid = (uint32_t)getpid();
    unsigned char identity[8] = {(unsigned char)(pid >> 24), (unsigned char)(pid >> 16),
                                 (unsigned char)(pid >> 8), (unsigned char)pid,
                                 (unsigned char)((pid + 1) >> 24),
                                 (unsigned char)((pid + 1) >> 16),
                                 (unsigned char)((pid + 1) >> 8), (unsigned char)(pid + 1)};
    int slow = strcmp(mode, "fragmented") == 0;
    frame('I', identity, sizeof(identity), slow);

    if (strcmp(mode, "duplicate") == 0) {
        frame('I', identity, sizeof(identity), 0);
    } else if (strcmp(mode, "duplicate_ready") == 0) {
        frame('R', NULL, 0, 0);
        frame('R', NULL, 0, 0);
    } else if (strcmp(mode, "duplicate_ast") == 0) {
        frame('R', NULL, 0, 0);
        frame('A', "{}", 2, 0);
        unsigned char verdict;
        if (read(0, &verdict, 1) != 1) return 2;
        frame('A', "{}", 2, 0);
    } else if (strcmp(mode, "truncated") == 0) {
        unsigned char partial[3] = {0, 0, 0};
        write_all(partial, sizeof(partial), 0);
    } else {
        frame('E', "o", 1, slow);
        if (strcmp(mode, "trailing") == 0) write_all("x", 1, 0);
    }
    return 0;
}
