/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 */

#include "spdk/queue_extras.h"
#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/blob.h"
#include "spdk/blob_bdev.h"
#include "spdk/rpc.h"
#include "../../../lib/blob/blobstore.h"
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/queue.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <errno.h>

static struct spdk_thread *g_init_thread;
static int g_pthread_id = 0;

#define JOBS_PER_PTHREAD 1
#define MAX_DEV 16

#define PTHREAD_NUM 1
#define DEFAULT_IO_DEPTH 4

struct task_pthread;
struct task_job;

struct dev_array {
    char *devname[MAX_DEV];
    int num_devs;
};

struct task_arg {
    int cpu;
    int pthread_id;
    int blk_size;
    int io_depth;
    struct dev_array *devs;
};
#define BLOB_NUM 8

struct task_job {
    int id;
    struct job_blobstore *job_bs;
    struct task_pthread *task_pthread;
    struct spdk_thread *thread;
    TAILQ_ENTRY(task_job) link;
    spdk_blob_id blob_array[BLOB_NUM];
    int blob_index;
};

struct task_pthread {
    int pthread_id;
    int depth;
    int blk_size;
    bool stop;
    uint64_t start_time;
    struct task_job *job_pool;
    TAILQ_HEAD(, task_job) jobs;
};

static void
task_thread_exit(struct spdk_thread *thread)
{
    spdk_set_thread(thread);
    /* exit init thread */
    spdk_thread_exit(thread);
    printf("thread %s is marking as exited.\n", spdk_thread_get_name(thread));

    do {
        spdk_thread_poll(thread, 0, 0);
    } while (!spdk_thread_is_exited(thread));
    spdk_thread_destroy(thread);
}


static void
job_destroy(struct task_job *job)
{
    spdk_set_thread(job->thread);

    task_thread_exit(job->thread);
    spdk_set_thread(NULL);
}

struct job_blobstore {
    struct spdk_bs_dev *bs_dev;
    struct spdk_io_channel *ch;
    bool failed;
    bool loaded;
    struct spdk_blob_store *bs;
    uint64_t nr_blob;
    uint64_t offset;
    uint32_t size;
    TAILQ_ENTRY(job_blobstore) link;
};

static void
inter_blob(void *ctx, struct spdk_blob *blob, int rc)
{
    struct job_blobstore *job_bs = ctx;
    if (rc) {
        SPDK_ERRLOG("load blob failed %d\n", rc);
        return;
    }

    SPDK_NOTICELOG("[%lu] blob %lu load successfully.\n", blob->bs->super_blob, blob->id);
    job_bs->nr_blob++;
}

static void
bs_load_complete(void *cb_arg, struct spdk_blob_store *bs, int bserrno)
{
    struct job_blobstore *job_bs = cb_arg;
    if (bserrno) {
        job_bs->failed = true;
        SPDK_ERRLOG("blobstore load failed.\n");
        return;
    }

    SPDK_NOTICELOG("blobstore load successfully. blobs %lu\n",
                job_bs->nr_blob);
    job_bs->bs = bs;
    job_bs->loaded = true;
    g_spdk_blob_stores = bs;
}

static void
event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		   void *event_ctx)
{
	printf("Unsupported bdev event: type %d on bdev %s\n", type, spdk_bdev_get_name(bdev));
}

static struct job_blobstore *
job_blobstore_create(struct task_job *job, char *name)
{
    struct job_blobstore *job_bs;
    struct spdk_bs_opts opts;

    job_bs = calloc(1, sizeof(*job_bs));
    if (!job_bs)
        return NULL;

    spdk_bdev_create_bs_dev_ext(name, event_cb, NULL,  &job_bs->bs_dev);
    if (job_bs->bs_dev == NULL) {
        SPDK_ERRLOG("%s, bs dev create failed.\n", name);
        goto failed;
    }


    spdk_bs_opts_init(&opts, sizeof(opts));
    opts.iter_cb_fn = inter_blob;
    opts.iter_cb_arg = job_bs;
    spdk_bs_load(job_bs->bs_dev, &opts, bs_load_complete, job_bs);

    while (!job_bs->failed && !job_bs->loaded) {
        spdk_thread_poll(job->thread, 0, 0);
    }
    return job_bs;

failed:
    free(job_bs);
    return NULL;
}

static int
job_create(struct task_job *job, struct dev_array *devs)
{
    int ret;
    char thread_name[100] = {0};

    sprintf(thread_name, "job_%d_%d", job->task_pthread->pthread_id, job->id);
    job->thread = spdk_thread_create(thread_name, NULL);
    if (job->thread == NULL) {
        printf("thread create failed for %s.\n", thread_name);
        return -ENOMEM;
    }

    ret = -ENOMEM;


    spdk_set_thread(job->thread);

    job->job_bs = job_blobstore_create(job, devs->devname[0]);
    if (job->job_bs == NULL)
        goto job_exit;

    spdk_set_thread(NULL);
    return 0;

job_exit:
    job_destroy(job);
    return ret;
}

static void
task_jobs_cleanup(struct task_pthread *task_thread)
{
    struct task_job *job, *tmp;
    TAILQ_FOREACH_SAFE(job, &task_thread->jobs, link, tmp) {
        TAILQ_REMOVE(&task_thread->jobs, job, link);
        job_destroy(job);
    }

    free(task_thread->job_pool);
}

static int
task_jobs_create(struct task_pthread *task_thread, struct dev_array *devs)
{
    struct task_job *job_pool;
    struct task_job *job;
    int ret = 0;
    int i;

    job_pool = calloc(JOBS_PER_PTHREAD, sizeof(struct task_job));
    if (job_pool == NULL)
        return -ENOMEM;

    task_thread->job_pool = job_pool;
    for (i = 0; i < JOBS_PER_PTHREAD; i++) {
        job = &job_pool[i];
        job->task_pthread = task_thread;
        job->id = i;
        ret = job_create(job, devs);
        if (ret)
            goto cleanup;
        TAILQ_INSERT_TAIL(&task_thread->jobs, job, link);
    }

    return 0;
cleanup:
    task_jobs_cleanup(task_thread);
    return -1;
}

static void
blob_create_cb(void *arg, spdk_blob_id blobid, int bserrno)
{
    struct task_job *job = arg;
    assert(bserrno == 0);

    printf("blob id %lu created.\n", blobid);
    job->blob_array[job->blob_index++] = blobid;
}

static void
blob_delete_cb(void *arg, int bserrno)
{
    struct task_job *job = arg;
    assert(bserrno == 0);

    job->blob_index--;
    //printf("blob deleted.\n");
    
}

void job_delete_blobs(struct task_job *job)
{
    int i = 0;
    int index = 0;

    index = job->blob_index;

    for (i = 0; i < BLOB_NUM; i++) {
        printf("deleting blob id %lu.\n", job->blob_array[index - 1]);
        spdk_bs_delete_blob(job->job_bs->bs, job->blob_array[index - 1],blob_delete_cb, job);
        index--;
    }

    while (1) {
        spdk_thread_poll(job->thread, 0, 0);
        if (job->blob_index == 0)
            break;
    }

}

void job_create_blobs(struct task_job *job)
{
    int i = 0;
    /* step create blobs */
    struct spdk_blob_opts opts;
    spdk_blob_opts_init(&opts, sizeof(opts));
    opts.thin_provision = false;
    opts.use_extent_table = false;
    opts.clear_method = 0;
    opts.num_clusters = 1025;


    for (i = 0; i < BLOB_NUM; i++) {
        spdk_bs_create_blob_ext(job->job_bs->bs, &opts, blob_create_cb, job);
    }

    while (1) {
        spdk_thread_poll(job->thread, 0, 0);
        if (job->blob_index == BLOB_NUM)
            break;
    }
}

void job_run_delete(struct task_job *job)
{
    int loop = 1000000;
    spdk_set_thread(job->thread);
    while (loop-- > 0) {
        job_create_blobs(job);
        job_delete_blobs(job);
    }
    //spdk_set_thread(NULL);
}

static void
bs_unload_complete(void *cb_arg, int bserrno)
{
    struct job_blobstore *job_bs = cb_arg;
    if (bserrno) {
        SPDK_ERRLOG("bs unload failed. %d\n", bserrno);
    }

    job_bs->loaded = false;
    job_bs->bs = NULL;
    free(job_bs);
}

int g_stop = false;

static void*
new_task(void *arg)
{
    struct task_arg *task_arg = arg;
    int cpu;
    cpu_set_t mask;
    int ret;
    struct dev_array *devs;
    struct task_pthread *task_thread;

    cpu = task_arg->cpu;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    ret = sched_setaffinity(0, sizeof(mask), &mask);
    if (ret) {
        printf("set cpu affinity failed.\n");
        goto out;
    }

    task_thread = calloc(1, sizeof(*task_thread));
    if (!task_thread) {
        printf("target alloc failed.\n");
        goto out;
    }

    task_thread->pthread_id = task_arg->pthread_id;
    task_thread->blk_size = task_arg->blk_size;
    task_thread->depth = task_arg->io_depth;
    TAILQ_INIT(&task_thread->jobs);

    devs = task_arg->devs;
    ret = task_jobs_create(task_thread, devs);
    if (ret) {
        printf("task jobs create failed.\n");
        goto out;
    }

    struct task_job *job;
    job = &task_thread->job_pool[0];
    job_run_delete(job);

    g_stop = true;
    spdk_bs_unload(job->job_bs->bs, bs_unload_complete, job->job_bs);
    task_jobs_cleanup(task_thread);

out:
    free(task_thread);
    return NULL;
}

static void
spdk_bdev_fini_done(void *arg)
{
    bool *done = arg;

    *done = true;
}

static void
spdk_bdev_init_done(int rc, void *arg)
{

    bool *done = arg;

    *done = true;
	if (rc) {
		bool tmp = false;
		SPDK_ERRLOG("spdk subsystem init fail %d\n", rc);
		spdk_subsystem_fini(spdk_bdev_fini_done, &tmp);
		return;
	}
	spdk_rpc_initialize(SPDK_DEFAULT_RPC_ADDR);
	spdk_rpc_set_state(SPDK_RPC_RUNTIME);
}

int
main(int argc, char **argv)
{
    int rc = 0;

    int i, j;
    bool init_done = 0;
	struct spdk_env_opts opts = {};
    struct spdk_thread *thread;
    pthread_t *new_pthreads;
    struct task_arg *args;
    struct dev_array *devs;
    int cores;

    if (argc < 3) {
        printf("parameter error.\n");
        return -1;
    }
    spdk_env_opts_init(&opts);
	opts.name = "lilei_spdk";
    opts.env_context = "--legacy-mem";

    j = 0;
    devs = calloc(1, sizeof(struct dev_array));
    if (!devs) {
        printf("dev calloc failed.\n");
        return -ENOMEM;
    }

    for (i = 2; i < argc; i++) {
        devs->devname[j++] = strdup(argv[i]);
    }
    devs->num_devs = j;

    rc = spdk_env_init(&opts);
    if (rc != 0) {
        printf("spdk env init failed.\n");
        goto free_devs;
    }

    spdk_unaffinitize_thread();
    spdk_thread_lib_init(NULL, 0);

    //spdk_log_set_flag("thread");
    spdk_log_set_print_level(SPDK_LOG_DEBUG);
    thread = spdk_thread_create("init_thread", NULL);
    if (thread == NULL) {
        printf("init thread create failed.\n");
        goto out;
    }

    spdk_set_thread(thread);
    g_init_thread = thread;
    /* setup subsystem */
    spdk_subsystem_init_from_json_config(argv[1], SPDK_DEFAULT_RPC_ADDR,
                                        spdk_bdev_init_done,  &init_done, true);
    do {
        spdk_thread_poll(thread, 0, 0);
    } while(init_done == false);

#if 1
    //cores = get_nprocs();
    cores = 1;
#else
    cores = PTHREAD_NUM;
#endif

    new_pthreads = calloc(cores, sizeof(pthread_t));
    args = calloc(cores, sizeof(struct task_arg));
    assert(new_pthreads);
    assert(args);

    for (i = 0; i < cores; i++) {
        args[i].cpu = i;
        args[i].pthread_id = g_pthread_id++;
        args[i].devs = devs;
        pthread_create(&new_pthreads[i], NULL, new_task, &args[i]);
    }

    do {
        spdk_thread_poll(thread, 0, 0);
        usleep(1);
    } while(g_stop == false);

    for (i = 0; i < cores; i++) {
        pthread_join(new_pthreads[i], NULL);
    }

    free(new_pthreads);
    free(args);

    spdk_rpc_finish();
    /* destroy subsystem */
    spdk_subsystem_fini(spdk_bdev_fini_done, &init_done);
    do {
        spdk_thread_poll(thread, 0, 0);
    } while(init_done == true);

    task_thread_exit(g_init_thread);
out:
    spdk_thread_lib_fini();
    spdk_env_fini();
free_devs:
    for (i = 0; i < devs->num_devs; i++) {
        free(devs->devname[i]);
    }
    free(devs);
	return rc;
}
