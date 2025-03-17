#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include "lab.h"
#ifdef __APPLE__
#include <sys/errno.h>
#else
#include <errno.h>
#endif
#include <assert.h>
#include <sys/mman.h>
#include <signal.h>
#define handle_error_and_die(msg) \
    do                            \
    {                             \
        perror(msg);              \
        raise(SIGKILL);           \
    } while (0)

#define UNUSED(x) (void)x

size_t btok(size_t bytes) {
    
    size_t kval = 0;

    while ((UINT64_C(1) << kval) < bytes) {
        kval++;
    }
    if (kval < SMALLEST_K) {
        kval = SMALLEST_K;
    }

    return kval;
}

struct avail *buddy_calc(struct buddy_pool *pool, struct avail *buddy) {

    uintptr_t pal = (uintptr_t)buddy - (uintptr_t)pool->base; 
    uintptr_t move = UINT64_C(1) << buddy->kval;
    uintptr_t mval = (pal ^ move);
    uintptr_t bestie = mval + (uintptr_t)pool->base;
    struct avail *retval = (struct avail *)bestie;

    return retval;
}

void *buddy_malloc(struct buddy_pool *pool, size_t size) {

    if (size == 0) {
        return NULL;
    }
    if (pool == NULL) {
        return NULL;
    }

    // to keep track of internal fragmentation
    pool->numcalls++;

    size_t kval = btok(size + sizeof(struct avail));
    assert(size < (UINT64_C(1) << kval));
    size_t j = kval;

    // Knuth R1
    while(j <= pool->kval_m && pool->avail[j].next == &pool->avail[j]) {
        j++;
    }

    if (j > pool->kval_m) {
        errno = ENOMEM;
        return NULL;
    }

    // lock critical section
    pthread_mutex_lock(&pool->lock);

    // Knuth R2
    struct avail *L = pool->avail[j].next;
    struct avail *P = L->next;
    pool->avail[j].next = P;
    P->prev = &pool->avail[j];
    L->tag = BLOCK_RESERVED;

    // Knuth R3 
    while (j != kval) {
        // Knuth R4
        j--;
        // to keep track of internal fragmentation
        pool->numsplits++;
        P = (struct avail*)((uintptr_t)L + (uintptr_t)(UINT64_C(1) << j));
        P->tag = BLOCK_AVAIL;
        P->kval = j;
        P->next = P->prev = &pool->avail[j];
        pool->avail[j].next = pool->avail[j].prev = P;
    }
    L->kval = kval;

    // unlock
    pthread_mutex_unlock(&pool->lock);

    // to keep track of internal fragmentation
    pool->numalloc += size;

    return L + 1;
}



void buddy_free(struct buddy_pool *pool, void *ptr) {
    
    // if ptr is null, do nothing
    if (ptr != nullptr) {
        
        // Knuth S1
        struct avail *L = (struct avail *)ptr - 1;
        struct avail *P = buddy_calc(pool, L);

        // Knuth S2
        while (!(L->kval == pool->kval_m || P->tag == BLOCK_RESERVED || (P->tag == BLOCK_AVAIL && P->kval != L->kval))) {
            // lock critical section
            pthread_mutex_lock(&pool->lock);

            P->prev->next = P->next;
            P->next->prev = P->prev;
            if (P < L) {
                L = P;
            }
            L->kval++;
            P = buddy_calc(pool, L);

            // unlock
            pthread_mutex_unlock(&pool->lock);
        }

        // lock critical section
        pthread_mutex_lock(&pool->lock);

        // Knuth S3
        L->tag = BLOCK_AVAIL;
        P = pool->avail[L->kval].next;
        L->next = P;
        P->prev = L;
        L->kval = L->kval;
        L->prev = &pool->avail[L->kval];
        pool->avail[L->kval].next = L;

        // unlock
        pthread_mutex_unlock(&pool->lock);
    }
}

void *buddy_realloc(struct buddy_pool *pool, void *ptr, size_t size) {

    if (ptr == NULL)
    {
        return buddy_malloc(pool, size);
    }

    if (size == 0)
    {
        buddy_free(pool, ptr);
        return NULL;
    }
    //Shift the pointer back to the header block so we can get at the header
    struct avail *block = (struct avail *)ptr - 1;
    size_t new_kval = btok(size + sizeof(struct avail));

    //Sanity check! If this fails it means we somehow handed out a block
    //of memory that was not marked as reserved. This means we probably
    //have a bug in buddy_malloc
    assert(block->tag == BLOCK_RESERVED);

    //Case 1: there is nothing to do we still have enough memory in the block
    //just return the pointer back unaltered.
    if (new_kval == block->kval)
    {
        return ptr;
    }

    //Case 2: We are growing or shrinking. This is a very brute force approach
    //grad students are required to make this better (see the writeup) undergrads
    //can use this as is :)
    void *rval = buddy_malloc(pool, size);
    // check if buddy is free
    struct avail *P = buddy_calc(pool, block);
    // if buddy is at a lesser address, move backwards and merge blocks
    if (P < rval) {
        rval = (void *)P;
        memmove(rval, ptr, size);
    } else {
        memmove(rval, ptr, size);
    }
    // memcpy(rval, ptr, size);
    buddy_free(pool, ptr);
    return rval;
}

void buddy_init(struct buddy_pool *pool, size_t size) {

    size_t kval = 0;
    if (size == 0)
        kval = DEFAULT_K;
    else
        kval = btok(size);

    if (kval < MIN_K)
        kval = MIN_K;
    if (kval > MAX_K)
        kval = MAX_K - 1;

    //make sure pool struct is cleared out
    memset(pool,0,sizeof(struct buddy_pool));
    pool->kval_m = kval;
    pool->numbytes = (UINT64_C(1) << pool->kval_m);
    //Memory map a block of raw memory to manage
    pool->base = mmap(
        NULL,                               /*addr to map to*/
        pool->numbytes,                     /*length*/
        PROT_READ | PROT_WRITE,             /*prot*/
        MAP_PRIVATE | MAP_ANONYMOUS,        /*flags*/
        -1,                                 /*fd -1 when using MAP_ANONYMOUS*/
        0                                   /* offset 0 when using MAP_ANONYMOUS*/
    );
    if (MAP_FAILED == pool->base)
    {
        handle_error_and_die("buddy_init avail array mmap failed");
    }

    //Set all blocks to empty. We are using circular lists so the first elements just point
    //to an available block. Thus the tag, and kval feild are unused burning a small bit of
    //memory but making the code more readable. We mark these blocks as UNUSED to aid in debugging.
    for (size_t i = 0; i <= kval; i++)
    {
        pool->avail[i].next = pool->avail[i].prev = &pool->avail[i];
        pool->avail[i].kval = i;
        pool->avail[i].tag = BLOCK_UNUSED;
    }

    pool->numalloc = 0;
    pool->numcalls = 0;
    pool->numsplits = 0;

    //Add in the first block
    pool->avail[kval].next = pool->avail[kval].prev = (struct avail *)pool->base;
    struct avail *m = pool->avail[kval].next;
    m->tag = BLOCK_AVAIL;
    m->kval = kval;
    m->next = m->prev = &pool->avail[kval];

    // Add lock for thread safety
    pthread_mutex_init(&pool->lock, NULL);
}

void buddy_destroy(struct buddy_pool *pool) {

    // destory lock
    pthread_mutex_destroy(&pool->lock);
    // reverse of mmap in init
    munmap(pool->base, pool->numbytes);
    // zero out struct
    memset(pool,0,sizeof(struct buddy_pool));
}

int go(int argc, char** argv) {

    UNUSED(argc);
    UNUSED(argv);

    return 0;
}
