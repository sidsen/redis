/*
* Copyright (c) 2011, Mathias Brossard <mathias@brossard.org>.
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are
* met:
*
*  1. Redistributions of source code must retain the above copyright
*     notice, this list of conditions and the following disclaimer.
*
*  2. Redistributions in binary form must reproduce the above copyright
*     notice, this list of conditions and the following disclaimer in the
*     documentation and/or other materials provided with the distribution.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
* A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
* HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/**
* @file threadpool.c
* @brief Threadpool implementation file
*/

#include <stdlib.h>
//#include <unistd.h>
#ifdef _WIN32
#include "win32_Interop/win32fixes.h"
#else
#include <pthread.h>
#endif
#include "threadpool.h"
#include "RDS.h"
#include "tmalloc.h"
#include "redis.h"

/**
*  @struct threadpool_task
*  @brief the work struct
*
*  @var function Pointer to the function that will perform the task.
*  @var argument Argument to be passed to the function.
*/

typedef struct {
	void(*function)(void *, int);
	void *argument;
} threadpool_task_t;

/**
*  @struct threadpool
*  @brief The threadpool struct
*
*  @var notify       Condition variable to notify worker threads.
*  @var threads      Array containing worker threads ID.
*  @var thread_count Number of threads
*  @var queue        Array containing the task queue.
*  @var queue_size   Size of the task queue.
*  @var head         Index of the first element.
*  @var tail         Index of the next element.
*  @var shutdown     Flag indicating if the pool is shutting down
*/
struct threadpool_t {
	pthread_mutex_t lock;
	pthread_cond_t notify;
	pthread_t *threads;
	threadpool_task_t *queue;
	int thread_count;
	int queue_size;
	int head;
	int tail;
	int count;
	int shutdown;
	int started;
};

/**
* @function void *threadpool_thread(void *threadpool)
* @brief the worker thread
* @param threadpool the pool which own the thread
*/
static void *threadpool_thread(void *threadpool);

int threadpool_free(threadpool_t *pool);

threadpool_t *threadpool_create(int thread_count, int queue_size, int flags)
{
	threadpool_t *pool;
	int i;

	/* TODO: Check for negative or otherwise very big input parameters */

	if ((pool = (threadpool_t *)malloc(sizeof(threadpool_t))) == NULL) {
		goto err;
	}

	/* Initialize */
	pool->thread_count = thread_count;
	pool->queue_size = queue_size;
	pool->head = pool->tail = pool->count = 0;
	pool->shutdown = pool->started = 0;

	/* Allocate thread and task queue */
	pool->threads = (pthread_t *)malloc(sizeof (pthread_t)* thread_count);
	pool->queue = (threadpool_task_t *)malloc
		(sizeof (threadpool_task_t)* queue_size);

	/* Initialize mutex and conditional variable first */
	if ((pthread_mutex_init(&(pool->lock), NULL) != 0) ||
		(pthread_cond_init(&(pool->notify), NULL) != 0) ||
		(pool->threads == NULL) ||
		(pool->queue == NULL)) {
		goto err;
	}

	/* Start worker threads */
	for (i = 0; i < thread_count; i++) {
		if (pthread_create(&(pool->threads[i]), NULL,
			threadpool_thread, (void*)pool) != 0) {
			threadpool_destroy(pool, 0);
			return NULL;
		}
		else {
			pool->started++;
		}
	}

	return pool;

err:
	if (pool) {
		threadpool_free(pool);
	}
	return NULL;
}

int threadpool_add(threadpool_t *pool, void(*function)(void *, int),
	void *argument, int flags)
{
	int err = 0;
	int next;

	if (pool == NULL || function == NULL) {
		return threadpool_invalid;
	}

	pthread_mutex_lock(&(pool->lock));
	//if (pthread_mutex_lock(&(pool->lock)) != 0) {
	//	return threadpool_lock_failure;
	//}

	next = pool->tail + 1;
	next = (next == pool->queue_size) ? 0 : next;

	do {
		/* Are we full ? */
		if (pool->count == pool->queue_size) {
			err = threadpool_queue_full;
			printf("Queue IS FULL!!\n");
			break;
		}

		/* Are we shutting down ? */
		if (pool->shutdown) {
			err = threadpool_shutdown;
			break;
		}

		/* Add task to queue */
		pool->queue[pool->tail].function = function;
		pool->queue[pool->tail].argument = argument;
		pool->tail = next;
		pool->count += 1;

		/* pthread_cond_broadcast */
		if (pthread_cond_signal(&(pool->notify)) != 0) {
			err = threadpool_lock_failure;
			break;
		}
	} while (0);

	pthread_mutex_unlock(&pool->lock);
	//if (pthread_mutex_unlock(&pool->lock) != 0) {
	//	err = threadpool_lock_failure;
	//}

	return err;
}

int threadpool_destroy(threadpool_t *pool, int flags)
{
	int i, err = 0;

	if (pool == NULL) {
		return threadpool_invalid;
	}

	pthread_mutex_lock(&(pool->lock));
	//if (pthread_mutex_lock(&(pool->lock)) != 0) {
	//	return threadpool_lock_failure;
	//}

	do {
		/* Already shutting down */
		if (pool->shutdown) {
			err = threadpool_shutdown;
			pthread_mutex_unlock(&pool->lock);
			break;
		}

		pool->shutdown = 1;

		/* Wake up all worker threads */
		//if ((pthread_cond_broadcast(&(pool->notify)) != 0) ||
		//	(pthread_mutex_unlock(&(pool->lock)) != 0)) {
		//	err = threadpool_lock_failure;
		//	break;
		//}
		if (pthread_cond_broadcast(&(pool->notify)) != 0) {			
			err = threadpool_lock_failure;
			pthread_mutex_unlock(&pool->lock);
			break;
		}
		pthread_mutex_unlock(&(pool->lock));

		/* Join all worker thread */
		for (i = 0; i < pool->thread_count; i++) {
			if (win32_pthread_join(pool->threads[i], NULL) != 0) {
				err = threadpool_thread_failure;
			}
		}
	} while (0);

	//if (pthread_mutex_unlock(&pool->lock) != 0) {
	//	err = threadpool_lock_failure;
	//}

	/* Only if everything went well do we deallocate the pool */
	if (!err) {
		threadpool_free(pool);
	}
	return err;
}

int threadpool_free(threadpool_t *pool)
{
	if (pool == NULL || pool->started > 0) {
		return -1;
	}

	/* Did we manage to allocate ? */
	if (pool->threads) {
		free(pool->threads);
		free(pool->queue);

		/* Because we allocate pool->threads after initializing the
		mutex and condition variable, we're sure they're
		initialized. Let's lock the mutex just in case. */
		pthread_mutex_lock(&(pool->lock));
		pthread_mutex_destroy(&(pool->lock));
		pthread_cond_destroy(&(pool->notify));
	}
	free(pool);
	return 0;
}


static void *threadpool_thread(void *threadpool)
{
	threadpool_t *pool = (threadpool_t *)threadpool;
	u32 thread_id = AtomicInc32(&threadCounter) - 1;

	/* Pin the thread */
	pinThread(thread_id);
	_tmreportprocessor(thread_id);
	/* The last thread is used for measurement and should not be associated with a replica */
	if ((server.repl || server.fc) && (thread_id != server.threadpool_size - 1)) {
		if (server.repl) {
			RDS_StartThread(rds, thread_id);
		} 
		else if (server.fc) {
			FC_StartThread(fc, thread_id);
		}
	}

	threadpool_task_t task;
	int opCount = 0;
	for (;;) {
		/* Lock must be taken to wait on conditional variable */
		pthread_mutex_lock(&(pool->lock));

		/* Wait on condition variable, check for spurious wakeups.
		When returning from pthread_cond_wait(), we own the lock. */
		while ((pool->count == 0) && (!pool->shutdown)) {
			pthread_cond_wait(&(pool->notify), &(pool->lock));
		}

		if (pool->shutdown) {
			break;
		}

		/* Grab our task */
		task.function = pool->queue[pool->head].function;
		task.argument = pool->queue[pool->head].argument;
		pool->head += 1;
		pool->head = (pool->head == pool->queue_size) ? 0 : pool->head;
		pool->count -= 1;

		/* Unlock */
		pthread_mutex_unlock(&(pool->lock));

		//printf("Thread %d has processed %d requests\n", thread_id, ++opCount);

		/* Set the thread id here (breaking abstraction, not great) */

		/* Get to work */
		(*(task.function))(task.argument, thread_id);

		//fprintf(stderr, "Thread %d is running on core %d\n", thread_id, GetCurrentProcessorNumber());
	}

	pool->started--;

	pthread_mutex_unlock(&(pool->lock));
	//pthread_exit(NULL);
	return(NULL);
}
