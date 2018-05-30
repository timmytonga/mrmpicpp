/* Author: Tim Nguyen (adopted from Buttenhof's Pthread book)
 * -- workq API Pthread
 * Allow to init a workq, destroy, and add tasks. */

#include "workq.h"
#include "cmake-build-debug/errors.h"
#include <stdlib.h>

/* initialize a workqueue:
 *  - Initialize condition variables
 *  - Initialize flags and workqueue */
int workq_init(workq_t *wq, int threads, void (*engine)(void*)){
    int status;
    /* initializing thread's attribute -> set detachable so that exits when complete  */
    status = pthread_attr_init(&wq->attr);
    if (status != 0) return status; // error
    status = pthread_attr_setdetachstate(&wq->attr, PTHREAD_CREATE_DETACHED);
    if (status != 0) {      // cannot set detach state, destroy and return error
        pthread_attr_destroy(&wq->attr);
        return status;
    }
    /* initialize condition and mutex variable -- check for errors and deallocate
     * if needed */
    status = pthread_cond_init(&wq->cv, NULL);
    if (status != 0) {
        pthread_attr_destroy(&wq->attr);
        return status;
    }
    status = pthread_mutex_init(&wq->mutex, NULL);
    if (status != 0){
        pthread_attr_destroy(&wq->attr);
        pthread_mutex_destroy(&wq->mutex);
        return status;
    }
    /* setup other workqueue attributes */
    wq->valid = WORKQ_VALID;    // this number is used to check if something is a workq
    wq->engine = engine;        // engine runs program
    wq->quit = 0;               // not yet quit
    wq->first = wq->last = NULL; // initialize an empty queue
    wq->parllelism = threads;   // number of threads determine how much to parallelize
    wq->idle = 0;               // no one is idle yet
    wq->counter = 0;            // no server thread yet
    return 0;
}

// threads perform this function which acts as a handler
// for engines to run --> more efficient for threads.
// it is passed a workqueue which in turns control what it does
/* MAIN FUNCTION THAT THREADS RUN.
 * Takes a workq_t element that contains conditional variables, job queue,
 *  info about other threads, quitting info, and other info require to run.
 * TAKE THE FIRST WORK FROM THE QUEUE WHEN THERE ARE WORK
 *  Otherwise wait and if long enough, exit. Finish work before shutting down. */
static void * workq_server(void* arg){
    struct timespec timeout;
    workq_t *wq = (workq_t*)arg;
    workq_ele_t *we;
    int status, timedout;

    DPRINTF(("A worker is starting\n"));
    // locking mutex to either wait (and terminate) or get a task to work on
    // have to modify the main workqueue so lock mutex
    status = pthread_mutex_lock(&wq->mutex);
    if (status != 0) return NULL;

    while(1){
        timedout = 0;
        DPRINTF(("Worker waiting for work\n"));
        clock_gettime(CLOCK_REALTIME, &timeout);
        timeout.tv_sec +=2;

        // When there's no work and we are not quitting yet... wait for work or if wait long enough
        // break and potentially checkout.
        while(wq->first == NULL && !wq->quit) {
            /* Server threads time out after spending 2 seconds waiting for new work and exit */
            status = pthread_cond_timedwait(&wq->cv, &wq->mutex, &timeout);
            if (status == ETIMEDOUT) {
                DPRINTF(("Worker wait timed out\n"));
                timedout = 1; // set flag to timed out (for condition underneath) --> break (still have lock)
                break;
            } else if (status != 0) { // strange error
                DPRINTF(("Worker wait failed, %d (%s)\n", status, strerror(status)));
                wq->counter--;
                pthread_mutex_unlock(&wq->mutex);
                return NULL;
            }
        }

        DPRINTF(("\tWork queue: %#lx, quit: %d\n", wq->first, wq->quit));
        we = wq->first;
        if (we != NULL){ // IF WE GOT AN ACTUAL WORK
            wq->first = wq->first->next; // increment queue
            if (wq->last == we) // if there was only one item in the queue
                wq->last = NULL;
            // finish obtaining work and modifying the queue
            // now we unlock the mutex and perform our work
            status = pthread_mutex_unlock(&wq->mutex);
            // run the engine on the data
            wq->engine(we->data);
            free(we); // remember to free the work element where we allocate in workq_add
            /* WE ARE DONE WITH MAIN WORK. Obtain the lock again and start the loop
             * one more time to either wait to terminate or to get a new task */
            status = pthread_mutex_lock(&wq->mutex); // obtaining the lock here
            // in case something failed we exit
            if (status != 0) return NULL;
        }

        /* before starting another loop, we have to check for some conditions
         * to see whether we should quit or terminate or not */
        if (wq->first == NULL && wq->quit){ // first we check if there's no more work and need to quit
            DPRINTF(("Worker shutting down\n"));
            wq->counter--;
            // here we signal workq_destroy that we have finished shutting down (it was waiting for threads to shut down)
            // hence wq->counter == 0.
            if (wq->counter == 0) pthread_cond_broadcast(&wq->cv);
            pthread_mutex_unlock(&wq->mutex);
            return NULL;
        }

        // here we timedout most likely came from above. We do this separately
        // because there might be a queue added in the meantime so we might do it again
        if (wq->first == NULL && timedout){
            DPRINTF(("Engine terminating due to timeout.\n"));
            wq->counter--;
            break;
        }
    }

    // end main while loop for work. We either returned from asking to quit (workq_destroy)
    // or we get here due to timedout. Otherwise, we should just keep grabbing work from the queue
    pthread_mutex_unlock(&wq->mutex);
    DPRINTF(("Worker exiting\n"));
    return 0;
}



int workq_add(workq_t *wq, void *element){

    workq_ele_t *item;
    pthread_t id;
    int status;

    if (wq->valid != WORKQ_VALID) return EINVAL;

    /* Create and initialize a request structure. */
    item = (workq_ele_t *)malloc( sizeof( workq_ele_t)); // free in server
    if (item == NULL) return ENOMEM; // out of memory cannot allocate
    item->data = element;
    item->next = NULL;
    /* now we add the request to the workqueue */
    status = pthread_mutex_lock(&wq->mutex);
    if (status != 0) { free(item); return status; }
    if (wq->first == NULL) wq->first = item;
    else wq->last->next = item;
    wq->last = item;

    /* Main job is done. Now we can signal idling threads to work or create more threads
     * else we just let the fully loaded crew to take tasks from the queue */
    if (wq->idle > 0) { // if there are any idling threads, wake them and return
        status = pthread_cond_signal(&wq->cv);
        if (status != 0){pthread_mutex_unlock(&wq->mutex);return status; }
    }
    else if (wq->counter < wq->parllelism){ // no idling thread -> we can create new threads if we have
        DPRINTF(("Creating new worker\n")); //  not reached the maximum number of threads
        status = pthread_create(&id, &wq->attr, workq_server, (void*)wq);
        if (status != 0 ) {pthread_mutex_unlock(&wq->mutex); return status;}
        wq->counter++;
    }
    pthread_mutex_unlock(&wq->mutex);

    return 0;
}




/* will accept a shutdown request at any time but will wait for
 * existing engine threads to complete their work and terminate */
int workq_destroy(workq_t *wq){

    int status, status1, status2;
    if (wq->valid != WORKQ_VALID) return EINVAL;

    status = pthread_mutex_lock(&wq->mutex);
    if (status != 0) return status;
    wq->valid = 0; // prevent any other operations
    /* Now we check if there are any active threads. If so, find them and shut down:
     *  1. set the quit flag
     *  2. broadcast to awake threads
     *  3. then wait for thread to quit due to quit flag
     *  -- no need for join to check thread id */
    if (wq->counter > 0 ){ // there are threads still doing stuff
        wq->quit = 1; // set the quit flag
        if (wq->idle > 0) {
            status = pthread_cond_broadcast(&wq->cv);
            if (status != 0) {pthread_mutex_unlock(&wq->mutex); return status;}
        }
        while (wq->counter > 0){ // keep waiting until the last thread signal
            status = pthread_cond_wait(&wq->cv, &wq->mutex);
            if (status != 0) {pthread_mutex_unlock(&wq->mutex); return status;}
        }
    }
    status = pthread_mutex_unlock(&wq->mutex); // done with quitting threads
    if (status != 0) return status;
    status = pthread_mutex_destroy(&wq->mutex);
    status1 = pthread_cond_destroy(&wq->cv);
    status2 = pthread_attr_destroy(&wq->attr);
    return (status ? status : (status1 ? status1 : status2));
}