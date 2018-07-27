//
// Created by timmytonga on 2/13/18.
//


#ifndef WORK_QUEUE_MANAGER_WORKQ_H
#define WORK_QUEUE_MANAGER_WORKQ_H

#include <pthread.h>
/* structure to keep track of work queue requests */
typedef struct workq_ele_tag{
    struct workq_ele_tag       *next;
    void                       *data;
} workq_ele_t;

/* structure describing a work queue */
typedef struct workq_tag{
    /* conditional variable for synchronization */
    pthread_mutex_t mutex;      // protect locking data
    pthread_cond_t  cv;         // use to signal when to start work and wait for work
    pthread_attr_t  attr;       // for detaching thread
    /* stuff for work queue*/
    workq_ele_t * first, *last; // linked queue for work
    int valid,                  // valid ?
            quit,                   // if != 0 then quit
            parllelism,             // maximum number of threads
            counter,                // current threads
            idle;                   // number of idle threads
    void    (*engine)(void *arg); // main function to perform work

} workq_t;

// initialize workqueue with maximum number of threads (threads)
// along with a routine function "engine"
extern int workq_init(workq_t *wq, int threads, void (*engine)(void*));

// destroy a workqueue
extern int workq_destroy(workq_t *wq);

extern int workq_add(workq_t *wq, void* data);

#define WORKQ_VALID 0x090997
#endif //WORK_QUEUE_MANAGER_WORKQ_H