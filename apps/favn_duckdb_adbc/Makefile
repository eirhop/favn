.PHONY: all
all:
	mkdir -p "$(MIX_APP_PATH)/priv"
	$(CC) $(CFLAGS) -std=c11 -D_POSIX_C_SOURCE=200809L -O2 -Wall -Wextra -Werror -pthread c_src/semantic_worker.c -o "$(MIX_APP_PATH)/priv/semantic_worker" -ldl
