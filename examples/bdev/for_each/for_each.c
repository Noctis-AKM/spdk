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
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/queue.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <errno.h>

static struct spdk_thread *g_init_thread;
static int g_pthread_id = 0;

/* each pthread manipulate 1G range */
#define PTHREAD_EXTENT_SIZE (1 << 30)
#define DEFAULT_BLK_SIZE (1 << 20)
/* each job manipulate 1G range */
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

struct dev_segment {
    struct spdk_bdev *bdev;
    struct spdk_bdev_desc *desc;
    struct spdk_io_channel *ch;
    uint64_t offset;
    uint32_t size;
    TAILQ_ENTRY(dev_segment) link;
};

struct job_ctx {
    struct task_job *job;
    struct spdk_bdev *bdev;
};

enum direction {
    IO_READ = 0,
    IO_WRITE,
};

struct job_io {
    enum direction direction;
    uint64_t offset;
    struct iovec iov;
    struct task_job *job;
    struct dev_segment *segment;
};

struct io_fifo {
    int front;
    int back;
    int mask;
    int size;
    long *data;
};

struct task_job {
    int id;
    struct task_pthread *task_pthread;
    struct spdk_thread *thread;
    struct job_io *io_pool;
    struct io_fifo io_fifo;
    int cur_depth;
    struct dev_segment *cur_seg;
    enum direction cur_dir;
    TAILQ_HEAD(, dev_segment) segments;
    TAILQ_ENTRY(task_job) link;
    char *buf_pool;
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

static int
io_fifo_used(struct task_job *job)
{
    struct io_fifo *fifo = &job->io_fifo;
    return ((fifo->back - fifo->front) & fifo->mask);
}

static int
io_fifo_free(struct task_job *job)
{
    struct io_fifo *fifo = &job->io_fifo;
    return fifo->size - io_fifo_used(job);
}

static bool
io_fifo_full(struct task_job *job)
{
    return !io_fifo_free(job);
}

static bool
io_fifo_empty(struct task_job *job)
{
    return !io_fifo_used(job);
}

static int
io_fifo_pop(struct task_job *job)
{
    int e;
    struct io_fifo *fifo = &job->io_fifo;
    if (io_fifo_empty(job))
        return -1;

    e = fifo->data[fifo->front++];
    fifo->front &= fifo->mask;
    return e;
}

static int io_fifo_push(struct task_job *job, int e)
{
    struct io_fifo *fifo = &job->io_fifo;

    if (io_fifo_full(job))
        return -1;

    fifo->data[fifo->back++] = e;
    fifo->back &= fifo->mask;
    return 0;
}

static void
io_fifo_destroy(struct task_job *job)
{
    struct io_fifo *fifo = &job->io_fifo;

    fifo->back = fifo->front = 0;
    fifo->size = 0;
    fifo->mask = 0;
    free(fifo->data);
}

char *g_buf;
static void
task_poll_second(struct spdk_thread *thread, int second)
{
    uint64_t timeout = spdk_get_ticks() + second * spdk_get_ticks_hz();
    do {
        spdk_thread_poll(thread, 0, 0);
    } while (spdk_get_ticks() < timeout);    
}

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

static struct job_io *
job_io_init(struct task_job *job, int index, int type)
{
    int blk_size = job->task_pthread->blk_size;
    struct job_io *io = &job->io_pool[index];

    io->iov.iov_base = job->buf_pool + (index * blk_size);
    io->iov.iov_len = blk_size;
    io->direction = type;
    io->job = job;

    return io;
}

static void
job_io_complete(struct spdk_bdev_io *io, bool success, void *arg)
{
    struct job_io *job_io = arg;
    struct task_job *job = job_io->job;
    int index = job_io - job->io_pool;
    int ret;

    job->cur_depth--;

#if 0
    printf("[%s][%s] IO %s done, offset 0x%lx, size 0x%lx, depth %d\n",
        spdk_thread_get_name(job->thread),
        spdk_bdev_get_name(job_io->segment->bdev),
        (job_io->direction == IO_WRITE) ? "write" : "read",
        job_io->offset, job_io->iov.iov_len, job->cur_depth);
#endif

    if (!success) {
        printf("io error.\n");
        job->task_pthread->stop = true;
    }

    if (io_fifo_push(job, index)) {
        printf("fifo full.\n");
        assert(0);
    }

#if 1
    if (job_io->direction == IO_READ) {
        char *tmp_buf = malloc(job_io->iov.iov_len);
        memset(tmp_buf, 0xca, job_io->iov.iov_len);
        ret = memcmp(job_io->iov.iov_base, tmp_buf, job_io->iov.iov_len);
        assert(ret == 0);    
        free(tmp_buf);
    }
#endif
    spdk_bdev_free_io(io);
    return;
}

static int
job_io_submit(struct dev_segment *segment, struct job_io *io)
{
    int ret;
    uint64_t offset;
    uint64_t len;
    struct spdk_bdev_desc *desc;
    struct spdk_io_channel *ch;

    desc = segment->desc;
    ch = segment->ch;
    offset = segment->offset;
    len = segment->size;
    io->offset = offset;
    io->segment = segment;

    assert(len = io->iov.iov_len);

    switch (io->direction) {
        case IO_WRITE:
            memset(io->iov.iov_base, 0xca, len);
            ret = spdk_bdev_write(desc, ch, io->iov.iov_base, offset, len,
                         job_io_complete, io);

            if (ret) {
                printf("bdev write error.\n");
                return -EIO;
            }
            break;
        case IO_READ:
#if 1
            memset(io->iov.iov_base, 0, len);
#endif
            ret = spdk_bdev_read(desc, ch, io->iov.iov_base, offset, len,
                        job_io_complete, io);
            if (ret) {
                printf("bdev read error.\n");
                return -EIO;
            }
            break;
        default:
            printf("unsupport op operation\n");
            assert(0);
    }

    return 0;
}

static int
job_do_rw(struct task_job *job)
{
    struct job_io *io;
    int index;
    int ret;

    if (job->cur_seg == NULL)
        job->cur_seg = TAILQ_FIRST(&job->segments);

    while (job->cur_seg != NULL) {
        index = io_fifo_pop(job);
        /* io fifo full, don't change direction */
        if (index == -1)
            return 0;

        job->cur_depth++;
        io = job_io_init(job, index, job->cur_dir);

        ret = job_io_submit(job->cur_seg, io);
        if (ret)
            return ret;

        job->cur_seg = TAILQ_NEXT(job->cur_seg, link);
    }

    /* change io direction after all segements io sent */
    job->cur_dir ^= 1;

    /* all segments have in flight io */
    return 0;
}

static void
job_per_channel_fn(struct spdk_bdev_channel_iter *i, struct spdk_bdev *bdev,
                struct spdk_io_channel *ch, void *ctx)
{
    struct job_ctx *job_ctx = ctx;

    printf("first at [%s], cur [%s], cpu %d bdev %s\n",
        spdk_thread_get_name(job_ctx->job->thread),
        spdk_thread_get_name(spdk_get_thread()), sched_getcpu(),
        spdk_bdev_get_name(bdev));

    spdk_bdev_for_each_channel_continue(i, 0);
}

static void
job_per_channel_done(struct spdk_bdev *bdev, void *ctx, int status)
{
    struct job_ctx *job_ctx = ctx;
    printf("END: first at [%s], cur [%s], cpu %d bdev %s\n",
        spdk_thread_get_name(job_ctx->job->thread),
        spdk_thread_get_name(spdk_get_thread()), sched_getcpu(),
        spdk_bdev_get_name(bdev));

    free(ctx);
}

static void
job_for_each_channel(struct task_job *job)
{
    struct job_ctx *ctx;
    struct dev_segment *segment;

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx)
        return;

    ctx->job = job;

    segment = TAILQ_FIRST(&job->segments);
    assert(segment);

    printf("START: first at [%s] cpu %d bdev %s\n",
        spdk_thread_get_name(job->thread), sched_getcpu(),
        spdk_bdev_get_name(segment->bdev));

    spdk_bdev_for_each_channel(segment->bdev, job_per_channel_fn, ctx, job_per_channel_done);
}

static void
run_job(struct task_job *job)
{
    int ret;

    spdk_set_thread(job->thread);
    if (job->cur_depth)
            return;

    ret = job_do_rw(job);
    if (ret)
        goto out;

    spdk_set_thread(NULL);
    return;

out:
    job->task_pthread->stop = true;
    spdk_set_thread(NULL);
}

static void
poll_job(struct task_job *job)
{
    spdk_set_thread(job->thread);

#if 0
    if ((job->task_pthread->stop == true) &&
        (!strcmp(spdk_thread_get_name(job->thread), "job_0_0")))
        job_for_each_channel(job);
#else
    if (job->task_pthread->stop == true)
        job_for_each_channel(job);
#endif

    while (1) {
        spdk_thread_poll(job->thread, 0, 0);
        if (job->cur_depth == 0)
            break;
    }
    spdk_set_thread(NULL);
}

static bool
thread_timeout(struct task_pthread *task_thread, int second)
{
    uint64_t timeout = task_thread->start_time + second * spdk_get_ticks_hz();
    uint64_t cur = spdk_get_ticks();

    return cur > timeout;
}

static void
run_jobs(struct task_pthread *task_thread)
{
    struct task_job *job;

    task_thread->start_time = spdk_get_ticks();
    while (task_thread->stop == false) {
        TAILQ_FOREACH(job, &task_thread->jobs, link) {
            if(task_thread->stop == true)
                break;
            run_job(job);
        }

        if (thread_timeout(task_thread, 8)) {
            printf("thread on CPU %d timeout.\n", sched_getcpu());
            task_thread->stop = true;
        }

        TAILQ_FOREACH(job, &task_thread->jobs, link) {
            poll_job(job);
        }
    }
}

static void
job_segment_del(struct dev_segment *segment)
{
    spdk_put_io_channel(segment->ch);
    spdk_bdev_close(segment->desc);
}

static void
job_segment_open_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		  void *event_ctx)
{
	printf("Unsupported bdev event: type %d\n", type);
}

static struct dev_segment *
job_segment_create(char *name)
{
    int ret;
    struct dev_segment *segment;

    segment = calloc(1, sizeof(*segment));
    if (!segment)
        return NULL;

    ret = spdk_bdev_open_ext(name, true, job_segment_open_cb,
                NULL, &segment->desc);
    if (ret) {
        printf("[%s] extent bdev open failed. ret %d\n", name, ret);
        goto failed;
    }

    segment->bdev = spdk_bdev_desc_get_bdev(segment->desc);
    segment->ch = spdk_bdev_get_io_channel(segment->desc);
    if (segment->ch == NULL) {
        printf("io channel get failed.\n");
        goto failed2;
    }

    return segment;
failed2:
    spdk_bdev_close(segment->desc);
failed:
    free(segment);
    return NULL;
}

static int
job_segment_add(struct task_job *job, char *name)
{
    struct dev_segment *segment;
    int size = job->task_pthread->blk_size;

    segment = job_segment_create(name);
    if (!segment)
        goto out;
    segment->offset = job->id * size;
    segment->size = size;
    TAILQ_INSERT_TAIL(&job->segments, segment, link);

    return 0;
out:
    return -1;
}

static void
job_destroy(struct task_job *job)
{
    struct dev_segment *segment, *tmp;
    spdk_set_thread(job->thread);

    TAILQ_FOREACH_SAFE(segment, &job->segments, link, tmp) {
        TAILQ_REMOVE(&job->segments, segment, link);
        job_segment_del(segment);
        free(segment);
    }

    io_fifo_destroy(job);
    free(job->io_pool);
    spdk_dma_free(job->buf_pool);
    task_thread_exit(job->thread);
    spdk_set_thread(NULL);
}

static int
io_fifo_init(struct task_job *job, int size)
{
    struct io_fifo *fifo = &job->io_fifo;
    int real_size = spdk_align32pow2(size + 1);

    fifo->data = calloc(real_size, sizeof(long));
    if (!fifo->data) {
        printf("alloc fifo io failed.\n");
        return -ENOMEM;
    }

    fifo->back = fifo->front = 0;
    fifo->size = real_size;
    fifo->mask = real_size - 1;

    return 0;
}

static int
job_create(struct task_job *job, struct dev_array *devs)
{
    int i, ret;
    int depth = job->task_pthread->depth;
    int blk_size = job->task_pthread->blk_size;
    char thread_name[100] = {0};

    sprintf(thread_name, "job_%d_%d", job->task_pthread->pthread_id, job->id);
    job->thread = spdk_thread_create(thread_name, NULL);
    if (job->thread == NULL) {
        printf("thread create failed for %s.\n", thread_name);
        return -ENOMEM;
    }
    TAILQ_INIT(&job->segments);

    ret = -ENOMEM;
    job->buf_pool = spdk_dma_zmalloc(depth * blk_size, blk_size, NULL);
    if (!job->buf_pool){
        printf("buf pool alloc failed.\n") ;
        task_thread_exit(job->thread);
        return ret;
    }
    printf("buffer pool addr 0x%lx\n", (unsigned long)job->buf_pool);

    /* io initialization */
    job->io_pool = calloc(depth, sizeof(struct job_io));
    if (!job->io_pool) {
        printf("alloc fifo io failed.\n");
        spdk_dma_free(job->buf_pool);
        task_thread_exit(job->thread);
        return ret;
    }

    ret = io_fifo_init(job, depth);
    if (ret) {
        printf("fifo io init failed.\n");
        free(job->io_pool);
        spdk_dma_free(job->buf_pool);
        task_thread_exit(job->thread);
        return ret;
    }

    for (i = 0; i < depth; i++) {
        /* fifo full must not be occured */
        ret = io_fifo_push(job, i);
        assert(ret == 0);
    }

    spdk_set_thread(job->thread);
    /* create segment */
    for (i = 0; i < devs->num_devs; i++) {
        ret = job_segment_add(job, devs->devname[i]);
        if (ret)
            goto job_exit;
    }

    /* write first */
    job->cur_dir = IO_WRITE;

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

    run_jobs(task_thread);
    task_jobs_cleanup(task_thread);
#if 0
    if (cpu == 1) {
        task_poll_second(target->thread, 1);
    }


out4:
    spdk_dma_free(read_buf);
out3:
    spdk_dma_free(write_buf);
out2:
    task_bdev_close(target);
out1:
    free(target);
#endif

out:
    free(task_thread);
    return NULL;
}

static void
spdk_bdev_fini_done(void *arg)
{
    bool *done = arg;

    *done = false;
}

static void
spdk_bdev_init_done(int rc, void *arg)
{
    bool *done = arg;

    *done = true;
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
    cores = get_nprocs();
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
        args[i].blk_size = DEFAULT_BLK_SIZE;
        args[i].io_depth = DEFAULT_IO_DEPTH;
        args[i].devs = devs;
        pthread_create(&new_pthreads[i], NULL, new_task, &args[i]);
    }

    for (i = 0; i < cores; i++) {
        pthread_join(new_pthreads[i], NULL);
    }

    free(new_pthreads);
    free(args);

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
