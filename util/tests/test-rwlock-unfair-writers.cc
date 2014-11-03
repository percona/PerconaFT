// check if write locks are fair

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <portability/toku_portability.h>
#include <portability/toku_pthread.h>

pthread_rwlock_t rwlock;
volatile int killed = 0;

static void *t1_func(void *arg) {
    int i;
    for (i = 0; !killed; i++) {
        toku_pthread_rwlock_wrlock(&rwlock); 
        toku_os_usleep(10000);
        toku_pthread_rwlock_rdunlock(&rwlock);
    }
    printf("%lu %d\n", (unsigned long) toku_pthread_self(), i);
    return arg;
}

int main(void) {
    int r;
    toku_pthread_rwlock_init(&rwlock, NULL);
    
    const int nthreads = 2;
    toku_pthread_t tids[nthreads];
    for (int i = 0; i < nthreads; i++) {
        r = toku_pthread_create(&tids[i], NULL, t1_func, NULL); 
        assert(r == 0);
    }
    toku_os_sleep(10);
    killed = 1;
    for (int i = 0; i < nthreads; i++) {
        void *ret;
        r = toku_pthread_join(tids[i], &ret);
        assert(r == 0);
    }
    return 0;
}
