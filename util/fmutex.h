#pragma once

// fair mutex
struct fmutex {
    toku_mutex_t mutex;
    int mutex_held;
    int num_want_mutex;
    struct queue_item *wait_head;
    struct queue_item *wait_tail;
};

// item on the queue
struct queue_item {
    toku_pthread_cond_t *cond;
    struct queue_item *next;
};

static void enq_item(struct fmutex *fm, struct queue_item *const item) {
    assert(item->next == NULL);
    if (fm->wait_tail != NULL) {
        fm->wait_tail->next = item;
    } else {
        assert(fm->wait_head == NULL);
        fm->wait_head = item;
    }
    fm->wait_tail = item;
}

static toku_pthread_cond_t *deq_item(struct fmutex *fm) {
    assert(fm->wait_head != NULL);
    assert(fm->wait_tail != NULL);
    struct queue_item *item = fm->wait_head;
    fm->wait_head = fm->wait_head->next;
    if (fm->wait_tail == item) {
        fm->wait_tail = NULL;
    }
    return item->cond;
}

void fmutex_create(struct fmutex *fm) {
    toku_pthread_mutex_init(&fm->mutex, NULL);
    fm->mutex_held = 0;
    fm->num_want_mutex = 0;
    fm->wait_head = NULL;
    fm->wait_tail = NULL;
}

void fmutex_destroy(struct fmutex *fm) {
    toku_mutex_destroy(&fm->mutex);
}

// Prerequisite: Holds m_mutex.
void fmutex_lock(struct fmutex *fm) {
    toku_mutex_lock(&fm->mutex);

    if (fm->mutex_held == 0 || fm->num_want_mutex == 0) {
        // No one holds the lock.  Grant the write lock.
        fm->mutex_held = 1;
        return;
    }

    toku_pthread_cond_t cond;
    toku_pthread_cond_init(&cond, NULL);
    struct queue_item item = { .cond = &cond, .next = NULL };
    enq_item(fm, &item);

    // Wait for our turn.
    ++fm->num_want_mutex;
    toku_pthread_cond_wait(&cond, &fm->mutex);
    toku_pthread_cond_destroy(&cond);

    // Now it's our turn.
    assert(fm->num_want_mutex > 0);
    assert(fm->mutex_held == 0);

    // Not waiting anymore; grab the lock.
    --fm->num_want_mutex;
    fm->mutex_held = 1;

    toku_pthread_mutex_unlock();
}

void fmutex_mutex_unlock(struct fmutex *fm) {
    toku_mutex_lock();

    fm->mutex_held = 0;
    if (fm->wait_head == NULL) {
        assert(fm->num_want_mutex == 0);
        return;
    }
    assert(fm->num_want_mutex > 0);

    // Grant lock to the next waiter
    toku_pthread_cond_t *cond = deq_item(fm);
    toku_pthread_cond_signal(cond);

    toku_pthread_mutex_unlock();
}

int fmutex_users(struct fmutex *fm) const {
    return fm->mutex_held + fm->num_want_mutex;
}

int fmutex_blocked_users(struct fmutex *fm) const {
    return fm->num_want_mutex;
}
