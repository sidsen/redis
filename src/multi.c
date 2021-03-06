/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"

/* ================================ MULTI/EXEC ============================== */

/* Client state initialization for MULTI/EXEC */
void initClientMultiState(redisClient *c) {
    c->mstate.commands = NULL;
    c->mstate.count = 0;
}

/* Release all the resources associated with MULTI/EXEC state */
void freeClientMultiState(redisClient *c) {
    int j;

    for (j = 0; j < c->mstate.count; j++) {
        int i;
        multiCmd *mc = c->mstate.commands+j;

        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);
        zfree(mc->argv);
    }
    zfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue */
void queueMultiCommand(redisClient *c) {
    multiCmd *mc;
    int j;

    c->mstate.commands = zrealloc(c->mstate.commands,
            sizeof(multiCmd)*(c->mstate.count+1));
    mc = c->mstate.commands+c->mstate.count;
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    mc->argv = zmalloc(sizeof(robj*)*c->argc);
    memcpy(mc->argv,c->argv,sizeof(robj*)*c->argc);
    for (j = 0; j < c->argc; j++)
        incrRefCount(mc->argv[j]);
    c->mstate.count++;
}

void discardTransaction(redisClient *c) {
    freeClientMultiState(c);
    initClientMultiState(c);
    c->flags &= ~(REDIS_MULTI|REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC);
    unwatchAllKeys(c);
}

/* Flag the transacation as DIRTY_EXEC so that EXEC will fail.
 * Should be called every time there is an error while queueing a command. */
void flagTransaction(redisClient *c) {
    if (c->flags & REDIS_MULTI)
        c->flags |= REDIS_DIRTY_EXEC;
}

void multiCommand(redisClient *c) {
    if (c->flags & REDIS_MULTI) {
        addReplyError(c,"MULTI calls can not be nested");
        return;
    }
    c->flags |= REDIS_MULTI;
    addReply(c,shared.ok);
}

void discardCommand(redisClient *c) {
    if (!(c->flags & REDIS_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }
    discardTransaction(c);
    addReply(c,shared.ok);
}

/* Send a MULTI command to all the slaves and AOF file. Check the execCommand
 * implementation for more information. */
void execCommandPropagateMulti(redisClient *c) {
    robj *multistring = createStringObject("MULTI",5);

    propagate(server.multiCommand,c->db->id,&multistring,1,
              REDIS_PROPAGATE_AOF|REDIS_PROPAGATE_REPL);
    decrRefCount(multistring);
}

void execCommand(redisClient *c) {
    int j;
    robj **orig_argv;
    int orig_argc;
    struct redisCommand *orig_cmd;
    int must_propagate = 0; /* Need to propagate MULTI/EXEC to AOF / slaves? */

    if (!(c->flags & REDIS_MULTI)) {
        addReplyError(c,"EXEC without MULTI");
        return;
    }

    /* Check if we need to abort the EXEC because:
     * 1) Some WATCHed key was touched.
     * 2) There was a previous error while queueing commands.
     * A failed EXEC in the first case returns a multi bulk nil object
     * (technically it is not an error but a special behavior), while
     * in the second an EXECABORT error is returned. */
    if (c->flags & (REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC)) {
        addReply(c, c->flags & REDIS_DIRTY_EXEC ? shared.execaborterr :
                                                  shared.nullmultibulk);
        discardTransaction(c);
        goto handle_monitor;
    }

    /* Exec all the queued commands */
    unwatchAllKeys(c); /* Unwatch ASAP otherwise we'll waste CPU cycles */
    orig_argv = c->argv;
    orig_argc = c->argc;
    orig_cmd = c->cmd;
    addReplyMultiBulkLen(c,c->mstate.count);
    for (j = 0; j < c->mstate.count; j++) {
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->cmd = c->mstate.commands[j].cmd;

        /* Propagate a MULTI request once we encounter the first write op.
         * This way we'll deliver the MULTI/..../EXEC block as a whole and
         * both the AOF and the replication link will have the same consistency
         * and atomicity guarantees. */
        if (!must_propagate && !(c->cmd->flags & REDIS_CMD_READONLY)) {
            execCommandPropagateMulti(c);
            must_propagate = 1;
        }

        call(c,REDIS_CALL_FULL);

        /* Commands may alter argc/argv, restore mstate. */
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].cmd = c->cmd;
    }
    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;
    discardTransaction(c);
    /* Make sure the EXEC command will be propagated as well if MULTI
     * was already propagated. */
    if (must_propagate) server.dirty++;

handle_monitor:
    /* Send EXEC to clients waiting data from MONITOR. We do it here
     * since the natural order of commands execution is actually:
     * MUTLI, EXEC, ... commands inside transaction ...
     * Instead EXEC is flagged as REDIS_CMD_SKIP_MONITOR in the command
     * table, and we do it here with correct ordering. */
    if (listLength(server.monitors) && !server.loading)
        replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
}

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called. */

/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB */
typedef struct watchedKey {
    robj *key;
    redisDb *db;
} watchedKey;

/* Watch for the specified key */
void watchForKey(redisClient *c, robj *key) {
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    /* Check if we are already watching for this key */
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key,wk->key))
            return; /* Key already watched */
    }
    /* This key is not already watched in this DB. Let's add it */
    clients = dictFetchValue(c->db->watched_keys,key);
    if (!clients) {
        clients = listCreate();
        dictAdd(c->db->watched_keys,key,clients);
        incrRefCount(key);
    }
    listAddNodeTail(clients,c);
    /* Add the new key to the list of keys watched by this client */
    wk = zmalloc(sizeof(*wk));
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    listAddNodeTail(c->watched_keys,wk);
}

/* Unwatch all the keys watched by this client. To clean the EXEC dirty
 * flag is up to the caller. */
void unwatchAllKeys(redisClient *c) {
    listIter li;
    listNode *ln;

    if (listLength(c->watched_keys) == 0) return;
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        list *clients;
        watchedKey *wk;

        /* Lookup the watched key -> clients list and remove the client
         * from the list */
        wk = listNodeValue(ln);
        clients = dictFetchValue(wk->db->watched_keys, wk->key);
        redisAssertWithInfo(c,NULL,clients != NULL);
        listDelNode(clients,listSearchKey(clients,c));
        /* Kill the entry at all if this was the only client */
        if (listLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);
        /* Remove this watched key from the client->watched list */
        listDelNode(c->watched_keys,ln);
        decrRefCount(wk->key);
        zfree(wk);
    }
}

/* "Touch" a key, so that if this key is being WATCHed by some client the
 * next EXEC will fail. */
void touchWatchedKey(redisDb *db, robj *key) {
    list *clients;
    listIter li;
    listNode *ln;

    if (dictSize(db->watched_keys) == 0) return;
    clients = dictFetchValue(db->watched_keys, key);
    if (!clients) return;

    /* Mark all the clients watching this key as REDIS_DIRTY_CAS */
    /* Check if we are already watching for this key */
    listRewind(clients,&li);
    while((ln = listNext(&li))) {
        redisClient *c = listNodeValue(ln);

        c->flags |= REDIS_DIRTY_CAS;
    }
}

/* On FLUSHDB or FLUSHALL all the watched keys that are present before the
 * flush but will be deleted as effect of the flushing operation should
 * be touched. "dbid" is the DB that's getting the flush. -1 if it is
 * a FLUSHALL operation (all the DBs flushed). */
void touchWatchedKeysOnFlush(int dbid) {
    listIter li1, li2;
    listNode *ln;

    /* For every client, check all the waited keys */
    listRewind(server.clients,&li1);
    while((ln = listNext(&li1))) {
        redisClient *c = listNodeValue(ln);
        listRewind(c->watched_keys,&li2);
        while((ln = listNext(&li2))) {
            watchedKey *wk = listNodeValue(ln);

            /* For every watched key matching the specified DB, if the
             * key exists, mark the client as dirty, as the key will be
             * removed. */
            if (dbid == -1 || wk->db->id == dbid) {
                if (dictFind(wk->db->dict, wk->key->ptr) != NULL)
                    c->flags |= REDIS_DIRTY_CAS;
            }
        }
    }
}

void watchCommand(redisClient *c) {
    int j;

    if (c->flags & REDIS_MULTI) {
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    }
    for (j = 1; j < c->argc; j++)
        watchForKey(c,c->argv[j]);
    addReply(c,shared.ok);
}

void unwatchCommand(redisClient *c) {
    unwatchAllKeys(c);
    c->flags &= (~REDIS_DIRTY_CAS);
    addReply(c,shared.ok);
}


/* ================================ BATCH (PIPELINE -> THREADPOOL) ============================== */

/* Client state initialization for batch processing */
void initClientBatchState(redisClient *c) {
	c->bstate.commands = NULL;
	c->bstate.count = 0;
}

/* Release all the resources associated with batch state */
void freeClientBatchState(redisClient *c) {
	int j;

	for (j = 0; j < c->bstate.count; j++) {
		int i;
		multiCmd *mc = c->bstate.commands + j;

		for (i = 0; i < mc->argc; i++)
			decrRefCount(mc->argv[i]);
		zfree(mc->argv);
	}
	zfree(c->bstate.commands);
}

/* Add a new command into the batch commands queue */
void queueBatchCommand(redisClient *c) {
	multiCmd *mc;
	int j;

	c->bstate.commands = zrealloc(c->bstate.commands,
		sizeof(multiCmd)*(c->bstate.count + 1));
	mc = c->bstate.commands + c->bstate.count;
	mc->cmd = c->cmd;
	mc->argc = c->argc;
	mc->argv = zmalloc(sizeof(robj*)*c->argc);
	memcpy(mc->argv, c->argv, sizeof(robj*)*c->argc);
	for (j = 0; j < c->argc; j++)
		incrRefCount(mc->argv[j]);
	c->bstate.count++;
}

void discardBatch(redisClient *c) {
	freeClientBatchState(c);
	initClientBatchState(c);
}

//SID-BENCH: CODE BELOW HAS BEEN MODIFIED FOR BENCHMARKING
__declspec(thread) struct multiCmd readCmd = { 0 };
__declspec(thread) struct multiCmd writeCmd = { 0 };
//u32* totalOps = NULL;
PaddedUInt totalOpss[MAX_THREADS];
volatile u16 trials = 0;
/* Counters used to synchronize threads between alternating trials */
//volatile u32 ready1 = 0;
//volatile u32 ready2 = 0;

//PaddedVolatileUInt ready3 = { 0, 0 };
//PaddedVolatileUInt ready4 = { 0, 0 };

PaddedUInt ready3 = { 0, 0 };
PaddedUInt ready4 = { 0, 0 };


void execBatch(redisClient *c) {
	int j;
	robj **orig_argv;
	int orig_argc;
	struct redisCommand *orig_cmd;
	
	/* Exec all the queued commands */
	orig_argv = c->argv;
	orig_argc = c->argc;
	orig_cmd = c->cmd;
	for (j = 0; j < c->bstate.count; j++) {
		c->argc = c->bstate.commands[j].argc;
		c->argv = c->bstate.commands[j].argv;
		c->cmd = c->bstate.commands[j].cmd;

		//SID: TEMPORARY CODE FOR BENCHMARKING
		if (readCmd.cmd == 0 && c->cmd->proc == zrankCommand) {		
			readCmd.argc = c->argc;
			readCmd.argv = c->argv;
			readCmd.cmd = c->cmd;			
		}
		if ((writeCmd.cmd == 0) && (c->cmd->proc == zincrbyCommand)) {
			writeCmd.argc = c->argc;
			writeCmd.argv = c->argv;
			writeCmd.cmd = c->cmd;
		}
		if (readCmd.cmd != 0 && writeCmd.cmd != 0)		
		{
			//float readRatios[] = { 0.0, 0.8, 0.9, 0.98, 1.0 };			
			float readRatios[] = {0.9};
			//volatile u32* volatile activeReady = &ready1;
			u32* volatile activeReady1 = &(ready3.val);
		

			for (int expCnt = 0; expCnt < sizeof(readRatios) / sizeof(float); expCnt++)
			{
				if (c->currthread == server.threadpool_size - 1) {
					server.exp_read_ratio = readRatios[expCnt];
					//totalOps = NULL;
					trials = 0;
					memset(totalOpss, 0, MAX_THREADS * sizeof(PaddedUInt));
				}

				/* Initialize the array for storing results */
				//if ((c->currthread == server.threadpool_size - 1) && (totalOps == NULL)) {
					//totalOps = zmalloc(server.threadpool_size * sizeof(u32));
					//memset(totalOps, 0, server.threadpool_size * sizeof(u32));					
				//}

				//activeReady = &ready1;
				/* Run the experiment exp-trials times */
				do {
					

					//AtomicInc32(activeReady);
					AtomicInc32(activeReady1);
					//while (*activeReady != server.threadpool_size) {
						//;
					//}

					while (*activeReady1 != server.threadpool_size) {
						_mm_pause();
					}

					float readRatio = server.exp_read_ratio;

					/* Use the extra trial round to synchronize the end of the experiment */
					if (trials == server.exp_trials) {
						if (c->currthread == server.threadpool_size - 1) {
							usleep(1000);
							//*activeReady = 0;
							*activeReady1 = 0;
						}
						else {
							//for (int j = 0; *activeReady != 0; j++)
							//	;
							while (*activeReady1 != 0) _mm_pause();
								
						}
						/* Switch to the other synchronization variable for the next experiment */
						//activeReady = (activeReady == &ready1) ? &ready2 : &ready1;
						activeReady1 = (activeReady1 == &(ready3.val)) ? &(ready4.val) : &(ready3.val);
						break;
					}

					if (c->currthread == (server.threadpool_size - 1)) {
						usleep(server.exp_duration_us);
						trials++;
						/* Indicates to all threads that the experiment is over */
						//*activeReady = 0;
						*activeReady1 = 0;
					}
					else {
						u32 i;
						//fprintf(stdout, "Starting operations on thread %d time is %d\n", c->currthread, server.exp_duration_us);
						//for (i = 0; *activeReady != 0; i++)
						for (i = 0; *activeReady1 != 0; i++)
						{
							//if (prng_next() <= readRatio * ULLONG_MAX)
							if (randLFSR() <= readRatio * USHRT_MAX)
							{
								c->argc = readCmd.argc;
								c->argv = readCmd.argv;
								c->cmd = readCmd.cmd;								
							}
							else
							{
								c->argc = writeCmd.argc;
								c->argv = writeCmd.argv;
								c->cmd = writeCmd.cmd;								
							}
							call(c, REDIS_CALL_FULL);
						}
//						totalOps[c->currthread] += i;
						totalOpss[c->currthread].val += i;
						//fprintf(stdout, "Total operations performed is %10d\n", i);
						//fflush(stdout);
					}
					/* Switch to the other synchronization variable for the next trial */
					//activeReady = (activeReady == &ready1) ? &ready2 : &ready1;
					activeReady1 = (activeReady1 == &(ready3.val)) ? &(ready4.val) : &(ready3.val);
				} while (trials <= server.exp_trials);  /* Enter loop once more than necessary to synchronize at end */

				/* Print the final results and exit redis */
				if (c->currthread == server.threadpool_size - 1) {
					u64 sumOps = 0;
					for (int j = 0; j < server.threadpool_size - 1; j++) {
						//sumOps += totalOps[j];
						sumOps += totalOpss[j].val;
					}
					fprintf(stdout, "Experiment results (threads = %d, trials = %d, duration = %d, keyrange = %d, read ratio = %f): %10f ops/sec\n",
						server.threadpool_size - 1, server.exp_trials, server.exp_duration_us, server.exp_keyrange, server.exp_read_ratio, sumOps / (server.exp_trials * (server.exp_duration_us / 1000000.0)));
					//fprintf(stdout, "%10f\n",
					//	sumOps / (server.exp_trials * (server.exp_duration_us / 1000000.0)));
					fflush(stdout);
					
				}
			}

			/* Mimic end below */
			c->argv = orig_argv;
			c->argc = orig_argc;
			c->cmd = orig_cmd;

			if (c->currthread == server.threadpool_size - 1) {
				//system("pause");
			}

			/* Exit redis to end the experiment */			
			exit(0);
		}

		//SIDTEMP: THE MEASURMENT THREAD SHOULD NOT INVOKE ANY OPS
		if (c->currthread != server.threadpool_size - 1) {
			call(c, REDIS_CALL_FULL);
		}
		else {
			addReplyLongLong(c, 1);
		}

		/* Commands may alter argc/argv, restore bstate. */
		c->bstate.commands[j].argc = c->argc;
		c->bstate.commands[j].argv = c->argv;
		c->bstate.commands[j].cmd = c->cmd;
	}
	c->argv = orig_argv;
	c->argc = orig_argc;
	c->cmd = orig_cmd;
}
