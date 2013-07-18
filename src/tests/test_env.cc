#include <stdio.h>
#include "toku_path.h"

// this test dumps out the programming environment.

int main(int argc, char *const argv[], char *const env[]) {
    for (int i = 0; i < argc; i++)
        printf("argv[%d]: %s\n", i, argv[i]);
    for (int i = 0; env[i]; i++)
        printf("env[%d]: %s\n", i, env[i]);
    printf("TOKU_TEST_FILENAME=%s\n", TOKU_TEST_FILENAME);
    return 0;
}
