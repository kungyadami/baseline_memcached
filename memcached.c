/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  memcached - memory caching daemon
 *
 *       https://www.memcached.org/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */
#include "memcached.h"
#include "storage.h"
#include "authfile.h"
#include "restart.h"
#include "slabs_mover.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <signal.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <ctype.h>
#include <stdarg.h>


/* some POSIX systems need the following definition
 * to get mlockall flags out of sys/mman.h.  */
#ifndef _P1003_1B_VISIBLE
#define _P1003_1B_VISIBLE
#endif
#include <pwd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sysexits.h>
#include <stddef.h>
#include <pthread.h>
#include <sys/time.h>
volatile int resp_num;
int completed_clients;
int mpi_client_size;
unsigned long hash_time[16];
unsigned long all_time;             //전체 시간 잴 때
struct timespec all_start, all_end; //전체 시간 잴 때
volatile int temp_rank=0;
unsigned long MPI_recv_time, MPI_send_time;
bool memcached_measure_enabled = false;
unsigned long MPI_get_hash_time = 0;
unsigned long MPI_get_hash_lookup_time = 0;
unsigned long MPI_get_build_resp_time = 0;
unsigned long MPI_state_machine_time = 0;
unsigned long MPI_parse_time = 0;
int old_hash_table_num=0;
int primary_hash_table_num=0;
int success = 0;
int get_req_num = 0;

#ifdef HAVE_GETOPT_LONG
#include <getopt.h> //프로그램 실행시 인자 처리 잘해주는 라이브러리
#endif

#include "tls.h"

#include "proto_text.h"
#include "proto_bin.h"
#include "proto_proxy.h"

#if defined(__FreeBSD__)
#include <sys/sysctl.h>
#endif

/*
 * forward declarations
 */
static void drive_machine(conn *c);
static int new_socket(struct addrinfo *ai);
static ssize_t tcp_read(conn *arg, void *buf, size_t count);
static ssize_t tcp_sendmsg(conn *arg, struct msghdr *msg, int flags);
static ssize_t tcp_write(conn *arg, void *buf, size_t count);

enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /** an error occurred (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /** failed to allocate more memory */
};

static int try_read_command_negotiate(conn *c);
static int try_read_command_udp(conn *c);

static enum try_read_result try_read_network(conn *c);
static enum try_read_result try_read_udp(conn *c);

static int start_conn_timeout_thread(void);

/* stats */
static void stats_init(void);
static void conn_to_str(const conn *c, char *addr, char *svr_addr);

/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void event_handler(const evutil_socket_t fd, const short which, void *arg);
static void conn_close(conn *c);
static void conn_init(void);
static bool update_event(conn *c, const int new_flags);
static void complete_nread(conn *c);

static void conn_free(conn *c);

/** exported globals **/
struct stats stats;
struct stats_state stats_state;
struct settings settings;
time_t process_started;     /* when the process was started */
conn **conns;

#ifdef EXTSTORE
/* hoping this is temporary; I'd prefer to cut globals, but will complete this
 * battle another day.
 */
void *ext_storage = NULL;
#endif
/** file scope variables **/
static conn *listen_conn = NULL;
static int max_fds;
static struct event_base *main_base;

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};




/*
* yedam's mpi function & disable socket functions macro
*/

#if ENABLE_MPI_FUNCTIONS

// #if HASH_BREAKDOWN
//     unsigned long hash_time[4];
//     volatile int temp_rank=0;
// #endif


    unsigned long send_brkdwn; //mpi_send/recv 할 때 성능
    unsigned long recv_brkdwn;
    unsigned long other_brkdwn; //그 외 각종 저장/토크나이즈 연산 성능
    struct timespec start, end;

#if BREAKDOWN
static unsigned long memcached_elapsed_ns(const struct timespec *start_time,
                                          const struct timespec *end_time) {
    return ((end_time->tv_sec - start_time->tv_sec) * BILLION)
            + end_time->tv_nsec - start_time->tv_nsec;
}

static void memcached_measure_start(void) {
    MPI_recv_time = 0;
    MPI_send_time = 0;
    send_brkdwn = 0;
    recv_brkdwn = 0;
    other_brkdwn = 0;
    MPI_get_hash_time = 0;
    MPI_get_hash_lookup_time = 0;
    MPI_get_build_resp_time = 0;
    MPI_state_machine_time = 0;
    MPI_parse_time = 0;
    memcached_measure_enabled = true;
}

static void memcached_measure_end(void) {
    if (!memcached_measure_enabled) {
        return;
    }

    memcached_measure_enabled = false;
    unsigned long categorized_time = MPI_parse_time + MPI_get_hash_time + MPI_get_hash_lookup_time + MPI_get_build_resp_time;
   // MPI_state_machine_time = other_brkdwn > categorized_time ? other_brkdwn - categorized_time : 0;
    printf("recv_time : %lu\n", MPI_recv_time);
    printf("send_time : %lu\n", MPI_send_time);
    printf("other_time : %lu\n", other_brkdwn);
    printf("get_hash_time : %lu\n", MPI_get_hash_time);
    printf("get_hash_lookup_time : %lu\n", MPI_get_hash_lookup_time);
    printf("get_build_resp_time : %lu\n", MPI_get_build_resp_time);
    printf("parse_time : %lu\n", MPI_parse_time);
}
#endif
    

// #define DATA_NUM 10000
// int nreqs;
// unsigned long mpi_thread_timer_read[NUM_MPI_RANKS];
// unsigned long mpi_thread_timer_write[NUM_MPI_RANKS];
// unsigned long mpi_thread_timer_etc[NUM_MPI_RANKS];
// unsigned long mpi_thread_timer_recv[NUM_MPI_RANKS];
// unsigned long mpi_thread_timer_send[NUM_MPI_RANKS];

// struct timespec start[NUM_MPI_RANKS];
// struct timespec end [NUM_MPI_RANKS];

void thread_func_for_mpi(void *c);

bool mpi_rbuf_switch_to_malloc(struct mpi_conn_info *c);

struct mpi_conn_info *mpi_conn_arr;

void mpi_conn_release_items(struct mpi_conn_info *c);

void mpi_conn_init(struct mpi_conn_info* mpi_conn_arr, int size);

static enum try_read_result mpi_try_read_network(struct mpi_conn_info* client);

mc_resp* mpi_resp_finish(struct mpi_conn_info *c, mc_resp *resp);

static void mpi_transmit_post(struct mpi_conn_info *c, ssize_t res);

static int mpi_transmit_pre(struct mpi_conn_info *c, struct iovec *iovs, int iovused, int tid);

ssize_t mpi_sendmsg(struct mpi_conn_info *c, int tid);

static bool mpi_rbuf_alloc(struct mpi_conn_info *c);

static void mpi_rbuf_release(struct mpi_conn_info *c);

static void mpi_reset_cmd_handler(struct mpi_conn_info *c);

void mpi_out_string(struct mpi_conn_info *c, const char *str);

static mc_resp* mpi_resp_allocate(struct mpi_conn_info *c);

bool mpi_resp_start(struct mpi_conn_info *c);

static void mpi_clock_handler(const evutil_socket_t fd, const short which, void *arg);

static void mpi_conn_close(struct mpi_conn_info *c);

static void mpi_drive_machine(struct mpi_conn_info *mpi_conn_arr, int tid);

#endif




















/* Default methods to read from/ write to a socket */
ssize_t tcp_read(conn *c, void *buf, size_t count) {
    assert (c != NULL);
#if ENABLE_PRINT
    printf("memcached/memcached.c :: tcp_read()\n");
#endif
    return read(c->sfd, buf, count);
}

    

ssize_t tcp_sendmsg(conn *c, struct msghdr *msg, int flags) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: tcp_sendmsg()\n");
#endif
    assert (c != NULL);

    ssize_t sent = sendmsg(c->sfd, msg, flags);
    return sent;
}

ssize_t tcp_write(conn *c, void *buf, size_t count) {
#if ENABLE_PRINT
        printf("memcached/memcached.c :: tcp_write()\n");
#endif
    assert (c != NULL);
    return write(c->sfd, buf, count);
}

static enum transmit_result transmit(conn *c);

/* This reduces the latency without adding lots of extra wiring to be able to
 * notify the listener thread of when to listen again.
 * Also, the clock timer could be broken out into its own thread and we
 * can block the listener via a condition.
 */
static volatile bool allow_new_conns = true;
static int stop_main_loop = NOT_STOP;
static struct event maxconnsevent;
static void maxconns_handler(const evutil_socket_t fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 0, .tv_usec = 10000};

    if (fd == -42 || allow_new_conns == false) {
        /* reschedule in 10ms if we need to keep polling */
        evtimer_set(&maxconnsevent, maxconns_handler, 0);
        event_base_set(main_base, &maxconnsevent);
        evtimer_add(&maxconnsevent, &t);
    } else {
        evtimer_del(&maxconnsevent);
        accept_new_conns(true);
    }
}

/*
 * given time value that's either unix time or delta from current unix time, return
 * unix time. Use the fact that delta can't exceed one month (and real time value can't
 * be that low).
 */
rel_time_t realtime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */
#if ENABLE_PRINT
    printf("memcached/memcached.c :: realtime 1\n");
#endif
    if (exptime == 0) return 0; /* 0 means never expire */

    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started){
#if ENABLE_PRINT
            printf("memcached/memcached.c :: realtime 2 return 1\n");
#endif
            return (rel_time_t)1;
        }
#if ENABLE_PRINT
        printf("memcached/memcached.c :: realtime 3 return %d\n", exptime - process_started);
#endif
        return (rel_time_t)(exptime - process_started);
    } else {
#if ENABLE_PRINT
        printf("memcached/memcached.c :: realtime 4 return %d\n", exptime + current_time);
#endif
        return (rel_time_t)(exptime + current_time);
    }
}

static void stats_init(void) {
#if ENABLE_PRINT
    printf("memcached/memcached.c -  :: stats_init()\n");
#endif
    memset(&stats, 0, sizeof(struct stats));
    memset(&stats_state, 0, sizeof(struct stats_state));
    stats_state.accepting_conns = true; /* assuming we start in this state. */

    /* make the time we started always be 2 seconds before we really
       did, so time(0) - time.started is never zero.  if so, things
       like 'settings.oldest_live' which act as booleans as well as
       values are now false in boolean context... */
    process_started = time(0) - ITEM_UPDATE_INTERVAL - 2;
    stats_prefix_init(settings.prefix_delimiter);
}

void stats_reset(void) {
    STATS_LOCK();
    memset(&stats, 0, sizeof(struct stats));
    stats_prefix_clear();
    STATS_UNLOCK();
    threadlocal_stats_reset();
    item_stats_reset();
}

static void settings_init(void) {
    settings.use_cas = true;
    settings.access = 0700;
    settings.port = 11211;
    settings.udpport = 0;
    ssl_init_settings();
    /* By default this string should be NULL for getaddrinfo() */
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024; /* default is 64MB */
    settings.maxconns = 1024;         /* to limit connections-related memory to about 5MB */
    settings.verbose = 0;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;       /* push old items out of cache when memory runs out */
    settings.socketpath = NULL;       /* by default, not using a unix socket */
    settings.auth_file = NULL;        /* by default, not using ASCII authentication tokens */
    settings.factor = 1.25;
    settings.chunk_size = 48;         /* space for a modest key and value */
    settings.num_threads = 1;         /* N workers */
    settings.num_threads_per_udp = 0;
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.reqs_per_event = 20;     /* 한 이벤트를 처리할 때 몇 개의 요청까지 처리할 지 설정하는 것*/
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
    settings.item_size_max = 1024 * 1024; /* The famous 1MB upper limit. */
    settings.slab_page_size = 1024 * 1024; /* chunks are split from 1MB pages. */
    settings.slab_chunk_size_max = settings.slab_page_size / 2;
    settings.sasl = false;
    settings.maxconns_fast = true;
    settings.lru_crawler = false;
    settings.lru_crawler_sleep = 100;
    settings.lru_crawler_tocrawl = 0;
    settings.lru_maintainer_thread = false;
    settings.lru_segmented = true;
    settings.hot_lru_pct = 20;
    settings.warm_lru_pct = 40;
    settings.hot_max_factor = 0.2;
    settings.warm_max_factor = 2.0;
    settings.temp_lru = false;
    settings.temporary_ttl = 61;
    settings.idle_timeout = 0; /* disabled */
    settings.hashpower_init = 0;
    settings.slab_reassign = true;
    settings.slab_automove = 1;
    settings.slab_automove_version = 0;
    settings.slab_automove_ratio = 0.8;
    settings.slab_automove_window = 10;
    settings.shutdown_command = false;
    settings.tail_repair_time = TAIL_REPAIR_TIME_DEFAULT;
    settings.flush_enabled = true;
    settings.dump_enabled = true;
    settings.crawls_persleep = 1000;
    settings.logger_watcher_buf_size = LOGGER_WATCHER_BUF_SIZE;
    settings.logger_buf_size = LOGGER_BUF_SIZE;
    settings.drop_privileges = false;
    settings.watch_enabled = true;
    settings.read_buf_mem_limit = 0;
#ifdef MEMCACHED_DEBUG
    settings.relaxed_privileges = false;
#endif
    settings.num_napi_ids = 0;
    settings.memory_file = NULL;
#ifdef SOCK_COOKIE_ID
    settings.sock_cookie_id = 0;
#endif
}

extern pthread_mutex_t conn_lock;

/* Connection timeout thread bits */
static pthread_t conn_timeout_tid;
static int do_run_conn_timeout_thread;
static pthread_cond_t conn_timeout_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t conn_timeout_lock = PTHREAD_MUTEX_INITIALIZER;

#define CONNS_PER_SLICE 100
static void *conn_timeout_thread(void *arg) {
    int i;
    conn *c;
    rel_time_t oldest_last_cmd;
    int sleep_time;
    int sleep_slice = max_fds / CONNS_PER_SLICE;
    if (sleep_slice == 0)
        sleep_slice = CONNS_PER_SLICE;

    useconds_t timeslice = 1000000 / sleep_slice;

    mutex_lock(&conn_timeout_lock);
    while(do_run_conn_timeout_thread) {
        if (settings.verbose > 2)
            fprintf(stderr, "idle timeout thread at top of connection list\n");

        oldest_last_cmd = current_time;

        for (i = 0; i < max_fds; i++) {
            if ((i % CONNS_PER_SLICE) == 0) {
                if (settings.verbose > 2)
                    fprintf(stderr, "idle timeout thread sleeping for %ulus\n",
                        (unsigned int)timeslice);
                usleep(timeslice);
            }

            if (!conns[i])
                continue;

            c = conns[i];

            if (!IS_TCP(c->transport))
                continue;

            if (c->state != conn_new_cmd && c->state != conn_read)
                continue;

            if ((current_time - c->last_cmd_time) > settings.idle_timeout) {
                timeout_conn(c);
            } else {
                if (c->last_cmd_time < oldest_last_cmd)
                    oldest_last_cmd = c->last_cmd_time;
            }
        }

        /* This is the soonest we could have another connection time out */
        sleep_time = settings.idle_timeout - (current_time - oldest_last_cmd) + 1;
        if (sleep_time <= 0)
            sleep_time = 1;

        if (settings.verbose > 2)
            fprintf(stderr,
                    "idle timeout thread finished pass, sleeping for %ds\n",
                    sleep_time);

        struct timeval now;
        struct timespec to_sleep;
        gettimeofday(&now, NULL);
        to_sleep.tv_sec = now.tv_sec + sleep_time;
        to_sleep.tv_nsec = 0;

        pthread_cond_timedwait(&conn_timeout_cond, &conn_timeout_lock, &to_sleep);
    }

    mutex_unlock(&conn_timeout_lock);
    return NULL;
}

static int start_conn_timeout_thread(void) {
    int ret;

    if (settings.idle_timeout == 0)
        return -1;

    do_run_conn_timeout_thread = 1;
    if ((ret = pthread_create(&conn_timeout_tid, NULL,
        conn_timeout_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create idle connection timeout thread: %s\n",
            strerror(ret));
        return -1;
    }
    thread_setname(conn_timeout_tid, "mc-idletimeout");

    return 0;
}

int stop_conn_timeout_thread(void) {
    if (!do_run_conn_timeout_thread)
        return -1;
    mutex_lock(&conn_timeout_lock);
    do_run_conn_timeout_thread = 0;
    pthread_cond_signal(&conn_timeout_cond);
    mutex_unlock(&conn_timeout_lock);
    pthread_join(conn_timeout_tid, NULL);
    return 0;
}

/*
 * read buffer cache helper functions
 */
static void rbuf_release(conn *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: rbuf_release\n");
#endif
    if (c->rbuf != NULL && c->rbytes == 0 && !IS_UDP(c->transport)) {
        if (c->rbuf_malloced) {
            free(c->rbuf);
            c->rbuf_malloced = false;
        } else {
            do_cache_free(c->thread->rbuf_cache, c->rbuf);
        }
        c->rsize = 0;
        c->rbuf = NULL;
        c->rcurr = NULL;
    }
}

static bool rbuf_alloc(conn *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: rbuf_alloc()\n");
#endif
    if (c->rbuf == NULL) {
        c->rbuf = do_cache_alloc(c->thread->rbuf_cache);
        if (!c->rbuf) {
            THR_STATS_LOCK(c->thread);
            c->thread->stats.read_buf_oom++;
            THR_STATS_UNLOCK(c->thread);
            return false;
        }
        c->rsize = READ_BUFFER_SIZE;
        c->rcurr = c->rbuf;
    }
    return true;
}

// Just for handling huge ASCII multigets.
// The previous system was essentially the same; realloc'ing until big enough,
// then realloc'ing back down after the request finished.
bool rbuf_switch_to_malloc(conn *c) {
    // Might as well start with x2 and work from there.
    size_t size = c->rsize * 2;
    char *tmp = malloc(size);
    if (!tmp)
        return false;

    memcpy(tmp, c->rcurr, c->rbytes);
    do_cache_free(c->thread->rbuf_cache, c->rbuf);

    c->rcurr = c->rbuf = tmp;
    c->rsize = size;
    c->rbuf_malloced = true;
    return true;
}

/*
 * Initializes the connections array. We don't actually allocate connection
 * structures until they're needed, so as to avoid wasting memory when the
 * maximum connection count is much higher than the actual number of
 * connections.
 *
 * This does end up wasting a few pointers' worth of memory for FDs that are
 * used for things other than connections, but that's worth it in exchange for
 * being able to directly index the conns array by FD.


 * connections 배열을 초기화합니다.  
* 실제로는 **필요해질 때까지 connection 구조체를 할당하지 않으며**,  
* 이는 최대 연결 수가 실제 연결 수보다 훨씬 클 때 **불필요한 메모리 낭비를 피하기 위함**입니다.
*
* 물론 연결이 아닌 용도로 사용되는 파일 디스크립터(FD)에 대해서는  
* 포인터 몇 개 정도의 메모리를 낭비하게 되지만,  
* **FD를 인덱스로 conns 배열에 직접 접근할 수 있다는 장점**을 고려하면  
* 이 정도의 낭비는 감수할 가치가 있습니다.

 */
static void conn_init(void) {
    //connection 배열을 초기화하는 코드라는데..
    /* We're unlikely to see an FD much higher than maxconns. */
#if ENABLE_PRINT
    printf("memcached/memcached.c :: conn_init()\n");
#endif
    int next_fd = dup(1);
    //1은 stdout(write, printf, puts..) 이라는 표준 출력 스트림인데,
    // 이걸 next_fd에 복제(근데 파일을 가리키는 건 같지만, 새로운 파일 디스크립터를 생성할 뿐임.
    // fd 번호는 다른 번호가 됨. 현재 미사용중인 번호 중 가장 작은 번호)
    if (next_fd < 0) {
        perror("Failed to duplicate file descriptor\n");
        exit(1);
    }
    int headroom = 10;      /* account for extra unexpected open FDs */
    struct rlimit rl;

    max_fds = settings.maxconns + headroom + next_fd;
    /*
        max_fds : 1051
        maxconns : 1024?
        headroom : 10
        next_fd : 17 //왜 17일까? -> 위에 이유 써져있다
        */


    /* But if possible, get the actual highest FD we can possibly ever see. */
    /* struct rlimit : 리눅스 시스템에서 프로세스 자원 제한을 나타내는 구조체
        rlimit.rlim_cur : soft limit,　현재 시스템이 적용하고 있는 제한값
        rlimit.rlim_max : hard limit, 사용자가 rlim_cur 값을 조절할 수 있는 최대치의 값
        
        RLIMIT_NOFILE : 리눅스에서 정의한 상수, 프로세스가 동시에 열 수 있는 최대 파일디스크립터 수를 뜻한다.
    */
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        max_fds = rl.rlim_max; //그래서 1051값은 1024를 넘기때문에, 1024로 다시 설정하는 부분
        //max_fds = 1024
    } else {
        fprintf(stderr, "Failed to query maximum file descriptor; "
                        "falling back to maxconns\n");
    }

    close(next_fd); //근데 왜닫지?

    if ((conns = calloc(max_fds, sizeof(conn *))) == NULL) {
        fprintf(stderr, "Failed to allocate connection structures\n");
        /* This is unrecoverable so bail out early. */
        exit(1);
    }
}

static const char *prot_text(enum protocol prot) {
    char *rv = "unknown";
    switch(prot) {
        case ascii_prot:
            rv = "ascii";
            break;
        case binary_prot:
            rv = "binary";
            break;
        case negotiating_prot:
            rv = "auto-negotiate";
            break;
#ifdef PROXY
        case proxy_prot:
            rv = "proxy";
            break;
#endif
    }
    return rv;
}

void conn_close_idle(conn *c) {
    if (settings.idle_timeout > 0 &&
        (current_time - c->last_cmd_time) > settings.idle_timeout) {
        if (c->state != conn_new_cmd && c->state != conn_read) {
            if (settings.verbose > 1)
                fprintf(stderr,
                    "fd %d wants to timeout, but isn't in read state", c->sfd);
            return;
        }

        if (settings.verbose > 1)
            fprintf(stderr, "Closing idle fd %d\n", c->sfd);

        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.idle_kicks++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        c->close_reason = IDLE_TIMEOUT_CLOSE;

        conn_set_state(c, conn_closing);
        drive_machine(c);
    }
}

static void _conn_event_readd(conn *c) {
    c->ev_flags = EV_READ | EV_PERSIST;
    event_set(&c->event, c->sfd, c->ev_flags, event_handler, (void *)c);
    event_base_set(c->thread->base, &c->event);

    // TODO: call conn_cleanup/fail/etc
    if (event_add(&c->event, 0) == -1) {
        perror("event_add");
    }
}

/* bring conn back from a sidethread. could have had its event base moved. */
void conn_worker_readd(conn *c) {
    assert(c->resps_suspended == 0); // TODO: remove assert.

    switch (c->state) {
        case conn_closing:
            drive_machine(c);
            break;
        case conn_io_pending:
            // The event listener was removed as more data showed up while
            // waiting for the async response.
            _conn_event_readd(c);
            // Explicit fall-through.
        case conn_io_queue:
            conn_set_state(c, conn_io_resume);
            // schedule the event, which just runs drive_machine outside of
            // any recursion here.
            event_active(&c->event, 0, 0);
            break;
        case conn_write:
        case conn_mwrite:
        case conn_read:
        case conn_parse_cmd:
            // No-ops if we weren't in a suspended state to begin with
            // TODO: which other states for this?
            break;
        default:
            event_del(&c->event);
            _conn_event_readd(c);
            conn_set_state(c, conn_new_cmd);
    }

}

void thread_io_queue_add(LIBEVENT_THREAD *t, int type, void *ctx, io_queue_stack_cb cb) {
    io_queue_t *q = t->io_queues;
    while (q->type != IO_QUEUE_NONE) {
        q++;
    }
    q->type = type;
    q->ctx = ctx;
    q->submit_cb = cb;
    STAILQ_INIT(&q->stack);
    return;
}

io_queue_t *thread_io_queue_get(LIBEVENT_THREAD *t, int type) {
    io_queue_t *q = t->io_queues;
    while (q->type != IO_QUEUE_NONE) {
        if (q->type == type) {
            return q;
        }
        q++;
    }
    return NULL;
}

void thread_io_queue_submit(LIBEVENT_THREAD *t) {
    t->conns_tosubmit = 0;
    for (io_queue_t *q = t->io_queues; q->type != IO_QUEUE_NONE; q++) {
        // submission callback must consume the queue
        if (!STAILQ_EMPTY(&q->stack)) {
            q->submit_cb(q);
            assert(STAILQ_EMPTY(&q->stack));
        }
    }
}

// called to return a single IO object to the original worker thread.
void conn_io_queue_return(io_pending_t *io) {
    io->return_cb(io);
}

conn *conn_new(const int sfd, enum conn_states init_state,
                const int event_flags,
                const int read_buffer_size, enum network_transport transport,
                struct event_base *base, void *ssl, uint64_t conntag,
                enum protocol bproto) {
    conn *c;
#if ENABLE_PRINT
    printf("memcached/memcached.c :: conn_new()-[1]\n");
#endif
    assert(sfd >= 0 && sfd < max_fds);
    c = conns[sfd];
    
    if (NULL == c) { //c가 null이라서 여기 들어감
        //conn 구조체의 객체 c를 할당한다. 성공해서 여기 조건문 안은 안 들어간다
        if (!(c = (conn *)calloc(1, sizeof(conn)))) {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            fprintf(stderr, "Failed to allocate connection object\n");
            return NULL;
        }
        MEMCACHED_CONN_CREATE(c);
        c->read = NULL; //read 연산을 할 때 함수 포인터 저장할 변수
        c->sendmsg = NULL; //얘도 sengmsg 연산을 하는 함수 포인터 저장하는 변수
        c->write = NULL; //얘도..
        c->rbuf = NULL; // 얘는 read 명령어 저장해주는 버퍼

        c->rsize = read_buffer_size;

        // UDP connections use a persistent static buffer.
        if (c->rsize) {
            c->rbuf = (char *)malloc((size_t)c->rsize);
            //rsize는 1의 값을 가지고 있어서, malloc으로 1byte 만큼의 크기를 할당해주는듯 ..
        }

        if (c->rsize && c->rbuf == NULL) {
            conn_free(c);
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            fprintf(stderr, "Failed to allocate buffers for connection\n");
            return NULL;
        }


        STATS_LOCK();
        stats_state.conn_structs++;
        STATS_UNLOCK();

        c->sfd = sfd;
        conns[sfd] = c;
    }

    c->transport = transport;
    c->protocol = bproto;
    c->tag = conntag;
    /* unix socket mode doesn't need this, so zeroed out.  but why
     * is this done for every command?  presumably for UDP
     * mode.  */
    if (!settings.socketpath) { //여기들어감
        c->request_addr_size = sizeof(c->request_addr); //왜 28이지?ㅋㅋㅋㅋㅋ
    } else {
        c->request_addr_size = 0;
    }
    if (transport == tcp_transport && init_state == conn_new_cmd) {
        //여기 안들어감 왜? init_state가 conn_new_cmd랑 값이 다름
        //소켓이 연결된 상대방의 주소 정보를 가져오는 함수
        if (getpeername(sfd, (struct sockaddr *) &c->request_addr,
                        &c->request_addr_size)) {
            perror("getpeername");
            memset(&c->request_addr, 0, sizeof(c->request_addr));
        }
    }
    if (init_state == conn_new_cmd) { //여기 안들어감
        //LOGGER_LOG(NULL, LOG_CONNEVENTS, LOGGER_CONNECTION_NEW, NULL, &c->request_addr, c->request_addr_size, c->transport, 0, sfd);
    }
    //init state : 0
    //c->protocol : 5
    //if (settings.verbose > 1) {
    //if (settings.verbose) {
        if (init_state == conn_listening) {
            //여기서 auto-negotiate 라는 걸 많이 출력하는데, 이건 자동으로 통신 방식, 설정을 맞춘다는 의미.
            //알아서 수신 대기 상태에 있는 소켓을 connection 배열에 넣어서, 연결 대기를 시킬 때 각종 네트워크 관련 설정을
            //알잘딱깔센으로 맞춰준다는 의미인듯?
#if ENABLE_FPRINT
            fprintf(stderr, "<%d server listening (%s)\n", sfd, prot_text(c->protocol));
#endif
        } else if (IS_UDP(transport)) {
#if ENABLE_FPRINT
            fprintf(stderr, "<%d server listening (udp)\n", sfd);
#endif
        } else if (c->protocol == negotiating_prot) {
#if ENABLE_FPRINT
            fprintf(stderr, "<%d new auto-negotiating client connection\n", sfd);
#endif
        } else if (c->protocol == ascii_prot) {
#if ENABLE_FPRINT
            fprintf(stderr, "<%d new ascii client connection.\n", sfd);
#endif
        } else if (c->protocol == binary_prot) {
#if ENABLE_FPRINT
            fprintf(stderr, "<%d new binary client connection.\n", sfd);
#endif
#ifdef PROXY
        } else if (c->protocol == proxy_prot) {
#if ENABLE_FPRINT
            fprintf(stderr, "<%d new proxy client connection.\n", sfd);
#endif
#endif
        } else {
#if ENABLE_FPRINT
            fprintf(stderr, "<%d new unknown (%d) client connection\n", sfd, c->protocol);
#endif
            assert(false);
        }
    //}
    c->state = init_state;
    c->rlbytes = 0;
    c->cmd = -1;
    c->rbytes = 0;
    c->rcurr = c->rbuf;
    c->ritem = 0;
    c->rbuf_malloced = false;
    c->item_malloced = false;
    c->sasl_started = false;
    c->set_stale = false;
    c->mset_res = false;
    c->close_after_write = false;
    c->last_cmd_time = current_time; /* initialize for idle kicker */
    assert(c->resps_suspended == 0);

    c->item = 0;
    c->ssl = NULL;
#ifdef TLS
    c->ssl_wbuf = NULL;
#endif

    c->noreply = false;

    if (ssl) {
        // musn't get here without ssl enabled.
        assert(settings.ssl_enabled);
        ssl_init_conn(c, ssl);
        c->ssl_enabled = true;
    } else { //여기로 들어감
        c->read = tcp_read;
        c->sendmsg = tcp_sendmsg;
        c->write = tcp_write;
        c->ssl_enabled = false;
    }

    if (IS_UDP(transport)) {
        c->try_read_command = try_read_command_udp;
    } else {
        switch (c->protocol) {
            case ascii_prot:
                if (settings.auth_file == NULL) {
                    c->authenticated = true;
                    c->try_read_command = try_read_command_ascii;
                } else {
                    c->authenticated = false;
                    c->try_read_command = try_read_command_asciiauth;
                }
                break;
            case binary_prot:
                // binprot handles its own authentication via SASL parsing.
                c->authenticated = false;
                c->try_read_command = try_read_command_binary;
                break;
            case negotiating_prot: //여기 실행됨
                c->try_read_command = try_read_command_negotiate; 
                break;
#ifdef PROXY
            case proxy_prot:
                c->try_read_command = try_read_command_proxy;
                break;
#endif
        }
    }
    //event는 libevent라고 하는 라이브러리
    //libevent는 비동기 I/O 처리를 좀 더 쉽게 할 수 있도록 하는 C 라이브러리
    //event_set은 주로 이벤트 구조체를 설정하는 함수
    //이벤트의 예시 : 소켓이 읽을 준비가 되었을 때 / 시그널이 도착했을 때 ..
    //어떤 fd(소켓 파일 디스크립터)에 어떤 일이 일어나는지를 미리 등록해두고, 그걸 이벤트로 사용해서 자동적으로 처리할 수 있도록 하는 것

    
    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    /*yedam start*/
    int fd = event_get_fd(&c->event);
    short flags = event_get_events(&c->event);
    /*yedam end*/

    if (event_add(&c->event, 0) == -1) {
        //c->event는 event 형 구조체인데, 여기 안에 이벤트 정보들이 담겨있음. 얘를 영구적으로 이벤트로 등록해두기
        perror("event_add");
        return NULL;
    }
    STATS_LOCK();
    stats_state.curr_conns++;
    stats.total_conns++;
    STATS_UNLOCK();

    MEMCACHED_CONN_ALLOCATE(c->sfd);

    return c;
}

void conn_release_items(conn *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: conn_release_items()\n");
#endif

    assert(c != NULL);

    if (c->item) {
        if (c->item_malloced) {
            free(c->item);
            c->item_malloced = false;
        } else {
            item_remove(c->item);
        }
        c->item = 0;
    }

    // Cull any unsent responses.
    if (c->resp_head) {     
        mc_resp *resp = c->resp_head;
        // r_f() handles the chain maintenance.
        while (resp) {
            // temporary by default. hide behind a debug flag in the future:
            // double free detection. Transmit loops can drop out early, but
            // here we could infinite loop.
            if (resp->free) {
                fprintf(stderr, "ERROR: double free detected during conn_release_items(): [%d] [%s]\n",
                        c->sfd, c->protocol == binary_prot ? "binary" : "ascii");
                // Since this is a critical failure, just leak the memory.
                // If these errors are seen, an abort() can be used instead.
                c->resp_head = NULL;
                c->resp = NULL;
                break;
            }
            resp = resp_finish(c, resp);
        }
    }
}

static void conn_cleanup(conn *c) {
    assert(c != NULL);

    conn_release_items(c);
#ifdef PROXY
    if (c->proxy_rctx) {
        proxy_cleanup_conn(c);
    }
#endif
    if (c->sasl_conn) {
        assert(settings.sasl);
        sasl_dispose(&c->sasl_conn);
        c->sasl_conn = NULL;
    }

    if (IS_UDP(c->transport)) {
        conn_set_state(c, conn_read);
    }
}

/*
 * Frees a connection.
 */
void conn_free(conn *c) {
    if (c) {
        assert(c != NULL);
        assert(c->sfd >= 0 && c->sfd < max_fds);

        MEMCACHED_CONN_DESTROY(c);
        conns[c->sfd] = NULL;
        if (c->rbuf)
            free(c->rbuf);
#ifdef TLS
        if (c->ssl_wbuf)
            c->ssl_wbuf = NULL;
#endif

        free(c);
    }
}

static void conn_close(conn *c) {
    assert(c != NULL);

    if (c->thread) {
        // LOGGER_LOG(c->thread->l, LOG_CONNEVENTS, LOGGER_CONNECTION_CLOSE, NULL,
        //         &c->request_addr, c->request_addr_size, c->transport,
        //         c->close_reason, c->sfd);
    }

    /* delete the event, the socket and the conn */
    event_del(&c->event);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d connection closed.\n", c->sfd);

    conn_cleanup(c);

    // force release of read buffer.
    if (c->thread) {
        c->rbytes = 0;
        rbuf_release(c);
    }

    MEMCACHED_CONN_RELEASE(c->sfd);
    conn_set_state(c, conn_closed);
    if (c->ssl_enabled) {
        ssl_conn_close(c->ssl);
    }
    close(c->sfd);
    c->close_reason = 0;
    pthread_mutex_lock(&conn_lock);
    allow_new_conns = true;
    pthread_mutex_unlock(&conn_lock);

    STATS_LOCK();
    stats_state.curr_conns--;
    STATS_UNLOCK();

    return;
}

// Since some connections might be off on side threads and some are managed as
// listeners we need to walk through them all from a central point.
// Must be called with all worker threads hung or in the process of closing.
void conn_close_all(void) {
    int i;
    for (i = 0; i < max_fds; i++) {
        if (conns[i] && conns[i]->state != conn_closed) {
            conn_close(conns[i]);
        }
    }
}

/**
 * Convert a state name to a human readable form.
 */
static const char *state_text(enum conn_states state) {
    const char* const statenames[] = { "conn_listening",
                                       "conn_new_cmd",
                                       "conn_waiting",
                                       "conn_read",
                                       "conn_parse_cmd",
                                       "conn_write",
                                       "conn_nread",
                                       "conn_swallow",
                                       "conn_closing",
                                       "conn_mwrite",
                                       "conn_closed",
                                       "conn_watch",
                                       "conn_io_queue",
                                       "conn_io_resume",
                                       "conn_io_pending" };
    return statenames[state];
}

/*
 * Sets a connection's current state in the state machine. Any special
 * processing that needs to happen on certain state transitions can
 * happen here.
 */
void conn_set_state(conn *c, enum conn_states state) {
    //printf("memcached/memcached.c :: conn_set_state()\n");
    assert(c != NULL);
    assert(state >= conn_listening && state < conn_max_state);

    if (state != c->state) {
        if (settings.verbose > 2) {
            fprintf(stderr, "%d: going from %s to %s\n",
                    c->sfd, state_text(c->state),
                    state_text(state));
        }

        if (state == conn_write || state == conn_mwrite) {
            MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->resp->wbuf, c->resp->wbytes);
        }
        c->state = state;
    }
}

/*
 * response object helper functions
 */
void resp_reset(mc_resp *resp) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: resp_reset()\n");
#endif
    if (resp->item) {

        item_remove(resp->item);
        resp->item = NULL;
    }
    if (resp->write_and_free) {
#ifdef PROXY
        if (resp->proxy_res) {
            LIBEVENT_THREAD *t = resp->bundle->thread;
            pthread_mutex_lock(&t->proxy_limit_lock);
            t->proxy_buffer_memory_used -= resp->wbytes;
            pthread_mutex_unlock(&t->proxy_limit_lock);
        }
#endif
        free(resp->write_and_free);
        resp->write_and_free = NULL;
    }
    resp->wbytes = 0;
    resp->tosend = 0;
    resp->iovcnt = 0;
    resp->chunked_data_iov = 0;
    resp->chunked_total = 0;
    resp->skip = false;
}

void resp_add_iov(mc_resp *resp, const void *buf, int len, int tid) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: resp_add_iov()\n");
#endif
#if MPI_DEBUG
    printf("memcached/memcached.c :: resp_add_iov(), buf : %s\n len : %d\n", buf, len);
#endif
    assert(resp->iovcnt < MC_RESP_IOVCOUNT);
    int x = resp->iovcnt;
#if MPI_DEBUG
    printf("\t [%d] x : %d\n", tid, x);
#endif
    resp->iov[x].iov_base = (void *)buf;
    resp->iov[x].iov_len = len;
    resp->iovcnt++;
#if MPI_DEBUG
    printf("\t [%d] resp->tosend(prev) : %d\n", tid, resp->tosend);
#endif
    resp->tosend += len;
#if MPI_DEBUG
    printf("\t [%d] resp->iovcnt : %d\n", tid, resp->iovcnt);
    printf("\t [%d] resp->iov[x].iov_len : %d\n", tid, resp->iov[x].iov_len);
    printf("\t [%d] resp->iov[x].iov_base : %s\n", tid, resp->iov[x].iov_base);
    printf("\t [%d] resp->tosend(next) : %d\n", tid, resp->tosend);
#endif
} 

// Notes that an IOV should be handled as a chunked item header.
// TODO: I'm hoping this isn't a permanent abstraction while I learn what the
// API should be.
void resp_add_chunked_iov(mc_resp *resp, const void *buf, int len) {
    resp->chunked_data_iov = resp->iovcnt;
    resp->chunked_total = len;
    resp_add_iov(resp, buf, len, 0);
}

// resp_allocate and resp_free are a wrapper around read buffers which makes
// read buffers the only network memory to track.
// Normally this would be too excessive. In this case it allows end users to
// track a single memory limit for ephemeral connection buffers.
// Fancy bit twiddling tricks are avoided to help keep this straightforward.
static mc_resp* resp_allocate(conn *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: resp_allocate()\n");
#endif
    LIBEVENT_THREAD *th = c->thread;
    mc_resp *resp = NULL;
    mc_resp_bundle *b = th->open_bundle;

    if (b != NULL) {
        for (int i = 0; i < MAX_RESP_PER_BUNDLE; i++) {
            // loop around starting from the most likely to be free
            int x = (i + b->next_check) % MAX_RESP_PER_BUNDLE;
            if (b->r[x].free) {
                resp = &b->r[x];
                b->next_check = x+1;
                break;
            }
        }

        if (resp != NULL) {
            b->refcount++;
            memset(resp, 0, sizeof(*resp));
            resp->free = false; // redundant, for clarity.
            resp->bundle = b;
            if (b->refcount == MAX_RESP_PER_BUNDLE) {
                assert(b->prev == NULL);
                // We only allocate off the head. Assign new head.
                th->open_bundle = b->next;
                // Remove ourselves from the list.
                if (b->next) {
                    b->next->prev = 0;
                    b->next = 0;
                }
            }
        }
    }

    if (resp == NULL) {
        assert(th->open_bundle == NULL);
        b = do_cache_alloc(th->rbuf_cache);
        if (b) {
            THR_STATS_LOCK(th);
            th->stats.response_obj_bytes += READ_BUFFER_SIZE;
            THR_STATS_UNLOCK(th);
            b->next_check = 1;
            b->refcount = 1;
            for (int i = 0; i < MAX_RESP_PER_BUNDLE; i++) {
                b->r[i].free = true;
            }
            b->next = 0;
            b->prev = 0;
            b->thread = th;
            th->open_bundle = b;
            resp = &b->r[0];
            memset(resp, 0, sizeof(*resp));
            resp->free = false; // redundant. for clarity.
            resp->bundle = b;
        } else {
            return NULL;
        }
    }
    return resp;
}

void resp_free(LIBEVENT_THREAD *th, mc_resp *resp) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: resp_free(), settings.oldest_live : %d\n", settings.oldest_live);
#endif
    //printf("resp_free\n");
    mc_resp_bundle *b = resp->bundle;

    resp->free = true;
    b->refcount--;
    if (b->refcount == 0) {
        //printf("resp_free1\n");
        if (b == th->open_bundle && b->next == 0) {
                //printf("resp_free2\n");
            // This is the final bundle. Just hold and reuse to skip init loop
            assert(b->prev == 0);
            b->next_check = 0;
        } else {
                //printf("resp_free3\n");
            // Assert that we're either in the list or at the head.
            assert((b->next || b->prev) || b == th->open_bundle);

            // unlink from list.
            mc_resp_bundle **head = &th->open_bundle;
            if (*head == b) *head = b->next;
            // Not tracking the tail.
            assert(b->next != b && b->prev != b);

            if (b->next) b->next->prev = b->prev;
            if (b->prev) b->prev->next = b->next;

            // Now completely done with this buffer.
            do_cache_free(th->rbuf_cache, b);
            THR_STATS_LOCK(th);
            th->stats.response_obj_bytes -= READ_BUFFER_SIZE;
            THR_STATS_UNLOCK(th);
        }
    } else {
            //printf("resp_free4\n");
        mc_resp_bundle **head = &th->open_bundle;
        // NOTE: since we're not tracking tail, latest free ends up in head.
        if (b == th->open_bundle || (b->prev || b->next)) {
                //printf("resp_free5\n");
            // If we're already linked, leave it in place to save CPU.
        } else {
                //printf("resp_free6\n");
            // Non-zero refcount, need to link into the freelist.
            b->prev = 0;
            b->next = *head;
            if (b->next) b->next->prev = b;
            *head = b;
        }

    }
    THR_STATS_LOCK(th);
    th->stats.response_obj_count--;
    THR_STATS_UNLOCK(th);
}

bool resp_start(conn *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: resp_start()\n");
#endif
    mc_resp *resp = resp_allocate(c);
    if (!resp) {
        THR_STATS_LOCK(c->thread);
        c->thread->stats.response_obj_oom++;
        THR_STATS_UNLOCK(c->thread);
        return false;
    }
    // handling the stats counters here to simplify testing
    THR_STATS_LOCK(c->thread);
    c->thread->stats.response_obj_count++;
    THR_STATS_UNLOCK(c->thread);
    if (!c->resp_head) {
        c->resp_head = resp;
    }
    if (!c->resp) {
        c->resp = resp;
    } else {
        c->resp->next = resp;
        c->resp = resp;
    }
    if (IS_UDP(c->transport)) {
        // need to hold on to some data for async responses.
        c->resp->request_id = c->request_id;
        c->resp->request_addr = c->request_addr;
        c->resp->request_addr_size = c->request_addr_size;
    }

    return true;
}

mc_resp *resp_start_unlinked(conn *c) {
    mc_resp *resp = resp_allocate(c);
    if (!resp) {
        THR_STATS_LOCK(c->thread);
        c->thread->stats.response_obj_oom++;
        THR_STATS_UNLOCK(c->thread);
        return false;
    }

    // handling the stats counters here to simplify testing
    THR_STATS_LOCK(c->thread);
    c->thread->stats.response_obj_count++;
    THR_STATS_UNLOCK(c->thread);

    if (IS_UDP(c->transport)) {
        // need to hold on to some data for async responses.
        c->resp->request_id = c->request_id;
        c->resp->request_addr = c->request_addr;
        c->resp->request_addr_size = c->request_addr_size;
    }

    return resp;
}

// returns next response in chain.
mc_resp* resp_finish(conn *c, mc_resp *resp) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: resp_finish()\n");
#endif
    mc_resp *next = resp->next;
    if (resp->item) {
        // TODO: cache hash value in resp obj?
        item_remove(resp->item);
        resp->item = NULL;
    }
    if (resp->write_and_free) {
#ifdef PROXY
        if (resp->proxy_res) {
            LIBEVENT_THREAD *t = resp->bundle->thread;
            pthread_mutex_lock(&t->proxy_limit_lock);
            t->proxy_buffer_memory_used -= resp->wbytes;
            pthread_mutex_unlock(&t->proxy_limit_lock);
        }
#endif
        free(resp->write_and_free);
    }
    if (resp->io_pending) {
        io_pending_t *io = resp->io_pending;
        // If we had a pending IO, tell it to internally clean up then return
        // the main object back to our thread cache.
        io->finalize_cb(io);
        do_cache_free(c->thread->io_cache, io);
        resp->io_pending = NULL;
    }
    if (c->resp_head == resp) { //여기
        c->resp_head = next;
    }
    if (c->resp == resp) { //여기
        c->resp = NULL;
    }
    resp_free(c->thread, resp);
    
    return next;
}

// tells if connection has a depth of response objects to process.
bool resp_has_stack(conn *c) {
    return c->resp_head->next != NULL ? true : false;
}

void out_string(conn *c, const char *str) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: out_string()\n");
#endif
    size_t len;
    assert(c != NULL);
    mc_resp *resp = c->resp;
    // if response was original filled with something, but we're now writing
    // out an error or similar, have to reset the object first.
    // TODO: since this is often redundant with allocation, how many callers
    // are actually requiring it be reset? Can we fast test by just looking at
    // tosend and reset if nonzero?
    resp_reset(resp);
    if (c->noreply) {
        // TODO: just invalidate the response since nothing's been attempted
        // to send yet?
        resp->skip = true;
        if (settings.verbose > 1)
            fprintf(stderr, ">%d NOREPLY %s\n", c->sfd, str);
        conn_set_state(c, conn_new_cmd);
        return;
    }

    if (settings.verbose > 1)
#if ENABLE_FPRINT
        fprintf(stderr, ">%d %s\n", c->sfd, str);
#endif
    // Fill response object with static string.

    len = strlen(str);
    if ((len + 2) > WRITE_BUFFER_SIZE) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = strlen(str);
    }
    memcpy(resp->wbuf, str, len);
    memcpy(resp->wbuf + len, "\r\n", 2);
    resp_add_iov(resp, resp->wbuf, len + 2, 0);
    conn_set_state(c, conn_new_cmd);
    return;
}

// For metaget-style ASCII commands. Ignores noreply, ensuring clients see
// protocol level errors.
void out_errstring(conn *c, const char *str) {
    c->noreply = false;
    out_string(c, str);
}

/*
 * Outputs a protocol-specific "out of memory" error. For ASCII clients,
 * this is equivalent to out_string().
 */
void out_of_memory(conn *c, char *ascii_error) {
    const static char error_prefix[] = "SERVER_ERROR ";
    const static int error_prefix_len = sizeof(error_prefix) - 1;

    if (c->protocol == binary_prot) {
        /* Strip off the generic error prefix; it's irrelevant in binary */
        if (!strncmp(ascii_error, error_prefix, error_prefix_len)) {
            ascii_error += error_prefix_len;
        }
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, ascii_error, 0);
    } else {
        out_string(c, ascii_error);
    }
}

static void append_bin_stats(const char *key, const uint16_t klen,
                             const char *val, const uint32_t vlen,
                             conn *c) {
    char *buf = c->stats.buffer + c->stats.offset;
    uint32_t bodylen = klen + vlen;
    protocol_binary_response_header header = {
        .response.magic = (uint8_t)PROTOCOL_BINARY_RES,
        .response.opcode = PROTOCOL_BINARY_CMD_STAT,
        .response.keylen = (uint16_t)htons(klen),
        .response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES,
        .response.bodylen = htonl(bodylen),
        .response.opaque = c->opaque
    };

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (klen > 0) {
        memcpy(buf, key, klen);
        buf += klen;

        if (vlen > 0) {
            memcpy(buf, val, vlen);
        }
    }

    c->stats.offset += sizeof(header.response) + bodylen;
}

static void append_ascii_stats(const char *key, const uint16_t klen,
                               const char *val, const uint32_t vlen,
                               conn *c) {
    char *pos = c->stats.buffer + c->stats.offset;
    uint32_t nbytes = 0;
    int remaining = c->stats.size - c->stats.offset;
    int room = remaining - 1;

    if (klen == 0 && vlen == 0) {
        nbytes = snprintf(pos, room, "END\r\n");
    } else if (vlen == 0) {
        nbytes = snprintf(pos, room, "STAT %s\r\n", key);
    } else {
        nbytes = snprintf(pos, room, "STAT %s %s\r\n", key, val);
    }

    c->stats.offset += nbytes;
}

static bool grow_stats_buf(conn *c, size_t needed) {
    size_t nsize = c->stats.size;
    size_t available = nsize - c->stats.offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (c->stats.buffer == NULL) {
        nsize = 1024;
        available = c->stats.size = c->stats.offset = 0;
    }

    while (needed > available) {
        assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - c->stats.offset;
    }

    if (nsize != c->stats.size) {
        char *ptr = realloc(c->stats.buffer, nsize);
        if (ptr) {
            c->stats.buffer = ptr;
            c->stats.size = nsize;
        } else {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            rv = false;
        }
    }

    return rv;
}

void append_stats(const char *key, const uint16_t klen,
                  const char *val, const uint32_t vlen,
                  const void *cookie)
{
    /* value without a key is invalid */
    if (klen == 0 && vlen > 0) {
        return;
    }

    conn *c = (conn*)cookie;

    if (c->protocol == binary_prot) {
        size_t needed = vlen + klen + sizeof(protocol_binary_response_header);
        if (!grow_stats_buf(c, needed)) {
            return;
        }
        append_bin_stats(key, klen, val, vlen, c);
    } else {
        size_t needed = vlen + klen + 10; // 10 == "STAT = \r\n"
        if (!grow_stats_buf(c, needed)) {
            return;
        }
        append_ascii_stats(key, klen, val, vlen, c);
    }

    assert(c->stats.offset <= c->stats.size);
}

static void reset_cmd_handler(conn *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c:: reset_cmd_handler start, c->resp_head : %d\n", c->resp_head);
#endif
    c->cmd = -1;
    c->substate = bin_no_state;
    if (c->item != NULL) {
        // TODO: Any other way to get here?
        // SASL auth was mistakenly using it. Nothing else should?
        if (c->item_malloced) {
            free(c->item);
            c->item_malloced = false;
        } else {
            item_remove(c->item);
        }
        c->item = NULL;
    }
    //상황에 따라 다르게 들어감, 주로 resp_head 아니면 else에 들어감
    if (c->rbytes > 0) { //파싱할 데이터가 있을 때
#if ENABLE_PRINT
        printf("memcached/memcached.c :: reset_cmd_handler4, c->rbytes > 0\n");
#endif
        conn_set_state(c, conn_parse_cmd);
    } else if (c->resp_head) { //보낼 메세지가 있을 때
#if ENABLE_PRINT
        printf("memcached/memcached.c :: reset_cmd_handler4, c->resp_head\n");
#endif
        conn_set_state(c, conn_mwrite);
    } else { //아무것도 아닐 때
#if ENABLE_PRINT
        printf("memcached/memcached.c :: reset_cmd_handler4, else\n");
#endif
        conn_set_state(c, conn_waiting);
    }
}

static void complete_nread(conn *c) {
    assert(c != NULL);
#ifdef PROXY
    assert(c->protocol == ascii_prot
           || c->protocol == binary_prot
           || c->protocol == proxy_prot);
#else
    assert(c->protocol == ascii_prot
           || c->protocol == binary_prot);
#endif
    if (c->protocol == ascii_prot) {
        complete_nread_ascii(c);
    } else if (c->protocol == binary_prot) {
        complete_nread_binary(c);
#ifdef PROXY
    } else if (c->protocol == proxy_prot) {
        complete_nread_proxy(c);
#endif
    }
}

/* Destination must always be chunked */
/* This should be part of item.c */
static int _store_item_copy_chunks(item *d_it, item *s_it, const int len) {
//chunked item의 데이터 영역에, 다른 item의 데이터를 이어서 복사하는 함수
    item_chunk *dch = (item_chunk *) ITEM_schunk(d_it);
    /* Advance dch until we find free space */
    while (dch->size == dch->used) {
        if (dch->next) {
            dch = dch->next;
        } else {
            break;
        }
    }

    if (s_it->it_flags & ITEM_CHUNKED) {
        int remain = len;
        item_chunk *sch = (item_chunk *) ITEM_schunk(s_it);
        int copied = 0;
        /* Fills dch's to capacity, not straight copy sch in case data is
         * being added or removed (ie append/prepend)
         */
        while (sch && dch && remain) {
            assert(dch->used <= dch->size);
            int todo = (dch->size - dch->used < sch->used - copied)
                ? dch->size - dch->used : sch->used - copied;
            if (remain < todo)
                todo = remain;
            memcpy(dch->data + dch->used, sch->data + copied, todo);
            dch->used += todo;
            copied += todo;
            remain -= todo;
            assert(dch->used <= dch->size);
            if (dch->size == dch->used) {
                item_chunk *tch = do_item_alloc_chunk(dch, remain);
                if (tch) {
                    dch = tch;
                } else {
                    return -1;
                }
            }
            assert(copied <= sch->used);
            if (copied == sch->used) {
                copied = 0;
                sch = sch->next;
            }
        }
        /* assert that the destination had enough space for the source */
        assert(remain == 0);
    } else {
        int done = 0;
        /* Fill dch's via a non-chunked item. */
        while (len > done && dch) {
            int todo = (dch->size - dch->used < len - done)
                ? dch->size - dch->used : len - done;
            //assert(dch->size - dch->used != 0);
            memcpy(dch->data + dch->used, ITEM_data(s_it) + done, todo);
            done += todo;
            dch->used += todo;
            assert(dch->used <= dch->size);
            if (dch->size == dch->used) {
                item_chunk *tch = do_item_alloc_chunk(dch, len - done);
                if (tch) {
                    dch = tch;
                } else {
                    return -1;
                }
            }
        }
        assert(len == done);
    }
    return 0;
}

static int _store_item_copy_data(int comm, item *old_it, item *new_it, item *add_it) {
    if (comm == NREAD_APPEND || comm == NREAD_APPENDVIV) {
        if (new_it->it_flags & ITEM_CHUNKED) {
            if (_store_item_copy_chunks(new_it, old_it, old_it->nbytes - 2) == -1 ||
                _store_item_copy_chunks(new_it, add_it, add_it->nbytes) == -1) {
                return -1;
            }
        } else {
            memcpy(ITEM_data(new_it), ITEM_data(old_it), old_it->nbytes);
            memcpy(ITEM_data(new_it) + old_it->nbytes - 2 /* CRLF */, ITEM_data(add_it), add_it->nbytes);
        }
    } else {
        /* NREAD_PREPEND */
        if (new_it->it_flags & ITEM_CHUNKED) {
            if (_store_item_copy_chunks(new_it, add_it, add_it->nbytes - 2) == -1 ||
                _store_item_copy_chunks(new_it, old_it, old_it->nbytes) == -1) {
                return -1;
            }
        } else {
            memcpy(ITEM_data(new_it), ITEM_data(add_it), add_it->nbytes);
            memcpy(ITEM_data(new_it) + add_it->nbytes - 2 /* CRLF */, ITEM_data(old_it), old_it->nbytes);
        }
    }
    return 0;
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. Protected by the item lock.
 *
 * Returns the state of storage.

  * set 계열 명령어의 의미에 따라
 * 아이템을 캐시에 저장합니다.  
 * 저장하는 동안은 item 락(item lock)으로 보호됩니다.
 *
 * 저장 결과 상태를 반환합니다.

 */
enum store_item_type do_store_item(item *it, int comm, LIBEVENT_THREAD *t, const uint32_t hv, int *nbytes, uint64_t *cas, uint64_t cas_in, bool cas_stale) {
    //여기 자체가 데이터를 저장하기 위해서 찾는 공간
    char *key = ITEM_key(it);
#if ENABLE_PRINT
    printf("memcached/memcached.c :: do_store_item(1)\n");
#endif
    item *old_it = do_item_get(key, it->nkey, hv, t, DONT_UPDATE);
    enum store_item_type stored = NOT_STORED;

    enum cas_result { CAS_NONE, CAS_MATCH, CAS_BADVAL, CAS_STALE, CAS_MISS };

    item *new_it = NULL;
    client_flags_t flags;

    /* Do the CAS test up front so we can apply to all store modes */
    enum cas_result cas_res = CAS_NONE;

    bool do_store = false;
    if (old_it != NULL) { //UPDATE 시 들어감
        // Most of the CAS work requires something to compare to.
        uint64_t it_cas = ITEM_get_cas(it);
        uint64_t old_cas = ITEM_get_cas(old_it);
        if (it_cas == 0) { //UPDATE할 때 들어감
            cas_res = CAS_NONE;
        } else if (it_cas == old_cas) { //안 들어감
            cas_res = CAS_MATCH;
        } else if (cas_stale && it_cas < old_cas) { //안 들어감
            cas_res = CAS_STALE;
        } else { //안 들어감
            cas_res = CAS_BADVAL;
        }

        switch (comm) {
            case NREAD_ADD: //안 들어감
                /* add only adds a nonexistent item, but promote to head of LRU */
                //본래 기존 key 가 없을 때만 넣는 명령인데, 현재는 있으므로 기존 item을 최근에 사용한 것처럼 update하고, LRU의 헤드로 이동시키는 것 같다.
                do_item_update(old_it);
                break;
            case NREAD_CAS: //안 들어감
                //얘는 CAS 저장하는 방식. 그래서 아까 계산한 CAS의 상태에 따라 아래의 로직대로 실행됨.
                if (cas_res == CAS_MATCH) {
                    // cas validates
                    // it and old_it may belong to different classes.
                    // I'm updating the stats for the one that's getting pushed out
                    pthread_mutex_lock(&t->stats.mutex);
                    t->stats.slab_stats[ITEM_clsid(old_it)].cas_hits++;
                    pthread_mutex_unlock(&t->stats.mutex);
                    do_store = true;
                } else if (cas_res == CAS_STALE) { //안 들어감
                    //STALE 모드일 경우에는 저장을 허용함.
                    // if we're allowed to set a stale value, CAS must be lower than
                    // the current item's CAS.
                    // This replaces the value, but should preserve TTL, and stale
                    // item marker bit + token sent if exists.
                    it->exptime = old_it->exptime;
                    it->it_flags |= ITEM_STALE;
                    if (old_it->it_flags & ITEM_TOKEN_SENT) {
                        it->it_flags |= ITEM_TOKEN_SENT;
                    }

                    pthread_mutex_lock(&t->stats.mutex);
                    t->stats.slab_stats[ITEM_clsid(old_it)].cas_hits++;
                    pthread_mutex_unlock(&t->stats.mutex);
                    do_store = true;
                } else { //안 들어감
                    // NONE or BADVAL are the same for CAS cmd
                    pthread_mutex_lock(&t->stats.mutex);
                    t->stats.slab_stats[ITEM_clsid(old_it)].cas_badval++;
                    pthread_mutex_unlock(&t->stats.mutex);

                    if (settings.verbose > 1) {
                        fprintf(stderr, "CAS:  failure: expected %llu, got %llu\n",
                                (unsigned long long)ITEM_get_cas(old_it),
                                (unsigned long long)ITEM_get_cas(it));
                    }
                    stored = EXISTS;
                }
                break;
            case NREAD_APPEND:
            case NREAD_PREPEND:
            case NREAD_APPENDVIV:
            case NREAD_PREPENDVIV: //이 부분은 기존의 값 뒤에 새 값을 붙이는 것.
                if (cas_res != CAS_NONE && cas_res != CAS_MATCH) { //CAS가 이상하면 걍 요청 거절하기
                    stored = EXISTS;
                    break;
                }
#ifdef EXTSTORE
                if ((old_it->it_flags & ITEM_HDR) != 0) { // 안 들어감
                    /* block append/prepend from working with extstore-d items.
                     * leave response code to NOT_STORED default */
                    break;
                }
#endif
                /* we have it and old_it here - alloc memory to hold both */
                FLAGS_CONV(old_it, flags);
                new_it = do_item_alloc(key, it->nkey, flags, old_it->exptime, it->nbytes + old_it->nbytes - 2 /* CRLF */);
                //기존값+새 값을 담을 item new_it 를 생성

                // OOM trying to copy.
                if (new_it == NULL)
                    break;
                /* copy data from it and old_it to new_it */
                //실제 데이터를 합치는 과정
                if (_store_item_copy_data(comm, old_it, new_it, it) == -1) {
                    //실패할 경우 걍 break
                    break;
                } else {//성공할 경우에는 old+new 를 합친 new_it를 it에 대입하고, 저장 여부를 참으로 변경
                    // 지금 이 시점에서 new_it의 refcount는 1임.
                    //link가 끝나면 2가 됨. 그리고 그 원래의 refcount 1이었던 참조는 이 함수 바깥부분에서 관리된다고 함.
                    it = new_it;
                    do_store = true;
                    // Upstream final object size for meta
                    if (nbytes != NULL) {
                        *nbytes = it->nbytes;
                    }
                }
                break;
            case NREAD_REPLACE:
            case NREAD_SET: //UPDATE 시 들어감
                do_store = true;
                break;
        }

        if (do_store) { //UPDATE시 들어감
            STORAGE_delete(t->storage, old_it);
            item_replace(old_it, it, hv, cas_in);
            stored = STORED;
        }

        do_item_remove(old_it);         /* release our reference */
        if (new_it != NULL) { //안 들어감
            // append/prepend end up with an extra reference for new_it.
            do_item_remove(new_it);
        }
    } else { ////데이터 삽입할 경우 들어감
        /* No pre-existing item to replace or compare to. */
        if (ITEM_get_cas(it) != 0) {
            /* Asked for a CAS match but nothing to compare it to. */
            cas_res = CAS_MISS;
        }

        switch (comm) {
            case NREAD_ADD:
            case NREAD_SET:
            case NREAD_APPENDVIV:
            case NREAD_PREPENDVIV:
                do_store = true;
                break;
            case NREAD_CAS:
                // LRU expired
                stored = NOT_FOUND;
                pthread_mutex_lock(&t->stats.mutex);
                t->stats.cas_misses++;
                pthread_mutex_unlock(&t->stats.mutex);
                break;
            case NREAD_REPLACE:
            case NREAD_APPEND:
            case NREAD_PREPEND:
                /* Requires an existing item. */
                break;
        }

        if (do_store) { //데이터 삽입할 경우 들어감
            //printf("memcached_server, do_store_item, it : %d\n", it);
            do_item_link(it, hv, cas_in);
            stored = STORED;
        }
    }

    if (stored == STORED && cas != NULL) { // ㅇ들어감
        *cas = ITEM_get_cas(it);
    }
    //yedam
    // LOGGER_LOG(t->l, LOG_MUTATIONS, LOGGER_ITEM_STORE, NULL,
    //         stored, comm, ITEM_key(it), it->nkey, it->nbytes, it->exptime,
    //         ITEM_clsid(it), t->cur_sfd);
    return stored;
}

/* set up a connection to write a buffer then free it, used for stats */
void write_and_free(conn *c, char *buf, int bytes) {
    if (buf) {
        mc_resp *resp = c->resp;
        resp->write_and_free = buf;
        resp_add_iov(resp, buf, bytes, 0);
        conn_set_state(c, conn_new_cmd);
    } else {
        out_of_memory(c, "SERVER_ERROR out of memory writing stats");
    }
}

void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...) {
    char val_str[STAT_VAL_LEN];
    int vlen;
    va_list ap;

    assert(name);
    assert(add_stats);
    assert(c);
    assert(fmt);

    va_start(ap, fmt);
    vlen = vsnprintf(val_str, sizeof(val_str) - 1, fmt, ap);
    va_end(ap);

    add_stats(name, strlen(name), val_str, vlen, c);
}

/* return server specific stats only */
void server_stats(ADD_STAT add_stats, void *c) {
    pid_t pid = getpid();
    rel_time_t now = current_time;

    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);
    struct slab_stats slab_stats;
    slab_stats_aggregate(&thread_stats, &slab_stats);
#ifndef WIN32
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
#endif /* !WIN32 */

    STATS_LOCK();

    APPEND_STAT("pid", "%lu", (long)pid);
    APPEND_STAT("uptime", "%u", now - ITEM_UPDATE_INTERVAL);
    APPEND_STAT("time", "%ld", now + (long)process_started);
    APPEND_STAT("version", "%s", VERSION);
    APPEND_STAT("libevent", "%s", event_get_version());
    APPEND_STAT("pointer_size", "%d", (int)(8 * sizeof(void *)));

#ifndef WIN32
    append_stat("rusage_user", add_stats, c, "%ld.%06ld",
                (long)usage.ru_utime.tv_sec,
                (long)usage.ru_utime.tv_usec);
    append_stat("rusage_system", add_stats, c, "%ld.%06ld",
                (long)usage.ru_stime.tv_sec,
                (long)usage.ru_stime.tv_usec);
#endif /* !WIN32 */

    APPEND_STAT("max_connections", "%d", settings.maxconns);
    APPEND_STAT("curr_connections", "%llu", (unsigned long long)stats_state.curr_conns - 1);
    APPEND_STAT("total_connections", "%llu", (unsigned long long)stats.total_conns);
    if (settings.maxconns_fast) {
        APPEND_STAT("rejected_connections", "%llu", (unsigned long long)stats.rejected_conns);
    }
    APPEND_STAT("connection_structures", "%u", stats_state.conn_structs);
    APPEND_STAT("response_obj_oom", "%llu", (unsigned long long)thread_stats.response_obj_oom);
    APPEND_STAT("response_obj_count", "%llu", (unsigned long long)thread_stats.response_obj_count);
    APPEND_STAT("response_obj_bytes", "%llu", (unsigned long long)thread_stats.response_obj_bytes);
    APPEND_STAT("read_buf_count", "%llu", (unsigned long long)thread_stats.read_buf_count);
    APPEND_STAT("read_buf_bytes", "%llu", (unsigned long long)thread_stats.read_buf_bytes);
    APPEND_STAT("read_buf_bytes_free", "%llu", (unsigned long long)thread_stats.read_buf_bytes_free);
    APPEND_STAT("read_buf_oom", "%llu", (unsigned long long)thread_stats.read_buf_oom);
    APPEND_STAT("reserved_fds", "%u", stats_state.reserved_fds);
#ifdef PROXY
    if (settings.proxy_enabled) {
        APPEND_STAT("proxy_conn_requests", "%llu", (unsigned long long)thread_stats.proxy_conn_requests);
        APPEND_STAT("proxy_conn_errors", "%llu", (unsigned long long)thread_stats.proxy_conn_errors);
        APPEND_STAT("proxy_conn_oom", "%llu", (unsigned long long)thread_stats.proxy_conn_oom);
        APPEND_STAT("proxy_req_active", "%llu", (unsigned long long)thread_stats.proxy_req_active);
    }
#endif
    APPEND_STAT("cmd_get", "%llu", (unsigned long long)thread_stats.get_cmds);
    APPEND_STAT("cmd_set", "%llu", (unsigned long long)slab_stats.set_cmds);
    APPEND_STAT("cmd_flush", "%llu", (unsigned long long)thread_stats.flush_cmds);
    APPEND_STAT("cmd_touch", "%llu", (unsigned long long)thread_stats.touch_cmds);
    APPEND_STAT("cmd_meta", "%llu", (unsigned long long)thread_stats.meta_cmds);
    APPEND_STAT("get_hits", "%llu", (unsigned long long)slab_stats.get_hits);
    APPEND_STAT("get_misses", "%llu", (unsigned long long)thread_stats.get_misses);
    APPEND_STAT("get_expired", "%llu", (unsigned long long)thread_stats.get_expired);
    APPEND_STAT("get_flushed", "%llu", (unsigned long long)thread_stats.get_flushed);
#ifdef EXTSTORE
    if (ext_storage) {
        APPEND_STAT("get_extstore", "%llu", (unsigned long long)thread_stats.get_extstore);
        APPEND_STAT("get_aborted_extstore", "%llu", (unsigned long long)thread_stats.get_aborted_extstore);
        APPEND_STAT("get_oom_extstore", "%llu", (unsigned long long)thread_stats.get_oom_extstore);
        APPEND_STAT("recache_from_extstore", "%llu", (unsigned long long)thread_stats.recache_from_extstore);
        APPEND_STAT("miss_from_extstore", "%llu", (unsigned long long)thread_stats.miss_from_extstore);
        APPEND_STAT("badcrc_from_extstore", "%llu", (unsigned long long)thread_stats.badcrc_from_extstore);
    }
#endif
    APPEND_STAT("delete_misses", "%llu", (unsigned long long)thread_stats.delete_misses);
    APPEND_STAT("delete_hits", "%llu", (unsigned long long)slab_stats.delete_hits);
    APPEND_STAT("incr_misses", "%llu", (unsigned long long)thread_stats.incr_misses);
    APPEND_STAT("incr_hits", "%llu", (unsigned long long)slab_stats.incr_hits);
    APPEND_STAT("decr_misses", "%llu", (unsigned long long)thread_stats.decr_misses);
    APPEND_STAT("decr_hits", "%llu", (unsigned long long)slab_stats.decr_hits);
    APPEND_STAT("cas_misses", "%llu", (unsigned long long)thread_stats.cas_misses);
    APPEND_STAT("cas_hits", "%llu", (unsigned long long)slab_stats.cas_hits);
    APPEND_STAT("cas_badval", "%llu", (unsigned long long)slab_stats.cas_badval);
    APPEND_STAT("touch_hits", "%llu", (unsigned long long)slab_stats.touch_hits);
    APPEND_STAT("touch_misses", "%llu", (unsigned long long)thread_stats.touch_misses);
    APPEND_STAT("store_too_large", "%llu", (unsigned long long)thread_stats.store_too_large);
    APPEND_STAT("store_no_memory", "%llu", (unsigned long long)thread_stats.store_no_memory);
    APPEND_STAT("auth_cmds", "%llu", (unsigned long long)thread_stats.auth_cmds);
    APPEND_STAT("auth_errors", "%llu", (unsigned long long)thread_stats.auth_errors);
    if (settings.idle_timeout) {
        APPEND_STAT("idle_kicks", "%llu", (unsigned long long)thread_stats.idle_kicks);
    }
    APPEND_STAT("bytes_read", "%llu", (unsigned long long)thread_stats.bytes_read);
    APPEND_STAT("bytes_written", "%llu", (unsigned long long)thread_stats.bytes_written);
    APPEND_STAT("limit_maxbytes", "%llu", (unsigned long long)settings.maxbytes);
    APPEND_STAT("accepting_conns", "%u", stats_state.accepting_conns);
    APPEND_STAT("listen_disabled_num", "%llu", (unsigned long long)stats.listen_disabled_num);
    APPEND_STAT("time_in_listen_disabled_us", "%llu", stats.time_in_listen_disabled_us);
    APPEND_STAT("threads", "%d", settings.num_threads);
    APPEND_STAT("conn_yields", "%llu", (unsigned long long)thread_stats.conn_yields);
    APPEND_STAT("hash_power_level", "%u", stats_state.hash_power_level);
    APPEND_STAT("hash_bytes", "%llu", (unsigned long long)stats_state.hash_bytes);
    APPEND_STAT("hash_is_expanding", "%u", stats_state.hash_is_expanding);
    if (settings.slab_reassign) {
        APPEND_STAT("slab_reassign_rescues", "%llu", stats.slab_reassign_rescues);
        APPEND_STAT("slab_reassign_chunk_rescues", "%llu", stats.slab_reassign_chunk_rescues);
        APPEND_STAT("slab_reassign_inline_reclaim", "%llu", stats.slab_reassign_inline_reclaim);
        APPEND_STAT("slab_reassign_busy_items", "%llu", stats.slab_reassign_busy_items);
        APPEND_STAT("slab_reassign_busy_deletes", "%llu", stats.slab_reassign_busy_deletes);
        APPEND_STAT("slab_reassign_busy_nomem", "%llu", stats.slab_reassign_busy_nomem);
        APPEND_STAT("slab_reassign_running", "%u", stats_state.slab_reassign_running);
        APPEND_STAT("slabs_moved", "%llu", stats.slabs_moved);
    }
    if (settings.lru_crawler) {
        APPEND_STAT("lru_crawler_running", "%u", stats_state.lru_crawler_running);
        APPEND_STAT("lru_crawler_starts", "%u", stats.lru_crawler_starts);
    }
    if (settings.lru_maintainer_thread) {
        APPEND_STAT("lru_maintainer_juggles", "%llu", (unsigned long long)stats.lru_maintainer_juggles);
    }
    APPEND_STAT("malloc_fails", "%llu",
                (unsigned long long)stats.malloc_fails);
    APPEND_STAT("log_worker_dropped", "%llu", (unsigned long long)stats.log_worker_dropped);
    APPEND_STAT("log_worker_written", "%llu", (unsigned long long)stats.log_worker_written);
    APPEND_STAT("log_watcher_skipped", "%llu", (unsigned long long)stats.log_watcher_skipped);
    APPEND_STAT("log_watcher_sent", "%llu", (unsigned long long)stats.log_watcher_sent);
    APPEND_STAT("log_watchers", "%llu", (unsigned long long)stats_state.log_watchers);
    STATS_UNLOCK();
#ifdef EXTSTORE
    storage_stats(add_stats, c);
#endif
#ifdef PROXY
    proxy_stats(settings.proxy_ctx, add_stats, c);
#endif
#ifdef TLS
    if (settings.ssl_enabled) {
        if (settings.ssl_session_cache) {
            APPEND_STAT("ssl_new_sessions", "%llu", (unsigned long long)stats.ssl_new_sessions);
        }
        APPEND_STAT("ssl_handshake_errors", "%llu", (unsigned long long)stats.ssl_handshake_errors);
        APPEND_STAT("ssl_proto_errors", "%llu", (unsigned long long)stats.ssl_proto_errors);
        APPEND_STAT("time_since_server_cert_refresh", "%u", now - settings.ssl_last_cert_refresh_time);
    }
#endif
    APPEND_STAT("unexpected_napi_ids", "%llu", (unsigned long long)stats.unexpected_napi_ids);
    APPEND_STAT("round_robin_fallback", "%llu", (unsigned long long)stats.round_robin_fallback);
}

void process_stat_settings(ADD_STAT add_stats, void *c) {
    assert(add_stats);
    APPEND_STAT("maxbytes", "%llu", (unsigned long long)settings.maxbytes);
    APPEND_STAT("maxconns", "%d", settings.maxconns);
    APPEND_STAT("tcpport", "%d", settings.port);
    APPEND_STAT("udpport", "%d", settings.udpport);
    APPEND_STAT("inter", "%s", settings.inter ? settings.inter : "NULL");
    APPEND_STAT("verbosity", "%d", settings.verbose);
    APPEND_STAT("oldest", "%lu", (unsigned long)settings.oldest_live);
    APPEND_STAT("evictions", "%s", settings.evict_to_free ? "on" : "off");
    APPEND_STAT("domain_socket", "%s",
                settings.socketpath ? settings.socketpath : "NULL");
    APPEND_STAT("umask", "%o", settings.access);
    APPEND_STAT("shutdown_command", "%s",
                settings.shutdown_command ? "yes" : "no");
    APPEND_STAT("growth_factor", "%.2f", settings.factor);
    APPEND_STAT("chunk_size", "%d", settings.chunk_size);
    APPEND_STAT("num_threads", "%d", settings.num_threads);
    APPEND_STAT("num_threads_per_udp", "%d", settings.num_threads_per_udp);
    APPEND_STAT("stat_key_prefix", "%c", settings.prefix_delimiter);
    APPEND_STAT("detail_enabled", "%s",
                settings.detail_enabled ? "yes" : "no");
    APPEND_STAT("reqs_per_event", "%d", settings.reqs_per_event);
    APPEND_STAT("cas_enabled", "%s", settings.use_cas ? "yes" : "no");
    APPEND_STAT("tcp_backlog", "%d", settings.backlog);
    APPEND_STAT("binding_protocol", "%s",
                prot_text(settings.binding_protocol));
    APPEND_STAT("auth_enabled_sasl", "%s", settings.sasl ? "yes" : "no");
    APPEND_STAT("auth_enabled_ascii", "%s", settings.auth_file ? settings.auth_file : "no");
    APPEND_STAT("item_size_max", "%d", settings.item_size_max);
    APPEND_STAT("maxconns_fast", "%s", settings.maxconns_fast ? "yes" : "no");
    APPEND_STAT("hashpower_init", "%d", settings.hashpower_init);
    APPEND_STAT("slab_reassign", "%s", settings.slab_reassign ? "yes" : "no");
    APPEND_STAT("slab_automove", "%d", settings.slab_automove);
    APPEND_STAT("slab_automove_ratio", "%.2f", settings.slab_automove_ratio);
    APPEND_STAT("slab_automove_window", "%u", settings.slab_automove_window);
    APPEND_STAT("slab_chunk_max", "%d", settings.slab_chunk_size_max);
    APPEND_STAT("lru_crawler", "%s", settings.lru_crawler ? "yes" : "no");
    APPEND_STAT("lru_crawler_sleep", "%d", settings.lru_crawler_sleep);
    APPEND_STAT("lru_crawler_tocrawl", "%lu", (unsigned long)settings.lru_crawler_tocrawl);
    APPEND_STAT("tail_repair_time", "%d", settings.tail_repair_time);
    APPEND_STAT("flush_enabled", "%s", settings.flush_enabled ? "yes" : "no");
    APPEND_STAT("dump_enabled", "%s", settings.dump_enabled ? "yes" : "no");
    APPEND_STAT("hash_algorithm", "%s", settings.hash_algorithm);
    APPEND_STAT("lru_maintainer_thread", "%s", settings.lru_maintainer_thread ? "yes" : "no");
    APPEND_STAT("lru_segmented", "%s", settings.lru_segmented ? "yes" : "no");
    APPEND_STAT("hot_lru_pct", "%d", settings.hot_lru_pct);
    APPEND_STAT("warm_lru_pct", "%d", settings.warm_lru_pct);
    APPEND_STAT("hot_max_factor", "%.2f", settings.hot_max_factor);
    APPEND_STAT("warm_max_factor", "%.2f", settings.warm_max_factor);
    APPEND_STAT("temp_lru", "%s", settings.temp_lru ? "yes" : "no");
    APPEND_STAT("temporary_ttl", "%u", settings.temporary_ttl);
    APPEND_STAT("idle_timeout", "%d", settings.idle_timeout);
    APPEND_STAT("watcher_logbuf_size", "%u", settings.logger_watcher_buf_size);
    APPEND_STAT("worker_logbuf_size", "%u", settings.logger_buf_size);
    APPEND_STAT("read_buf_mem_limit", "%u", settings.read_buf_mem_limit);
    APPEND_STAT("track_sizes", "%s", item_stats_sizes_status() ? "yes" : "no");
    APPEND_STAT("inline_ascii_response", "%s", "no"); // setting is dead, cannot be yes.
#ifdef HAVE_DROP_PRIVILEGES
    APPEND_STAT("drop_privileges", "%s", settings.drop_privileges ? "yes" : "no");
#endif
#ifdef EXTSTORE
    APPEND_STAT("ext_item_size", "%u", settings.ext_item_size);
    APPEND_STAT("ext_item_age", "%u", settings.ext_item_age);
    APPEND_STAT("ext_low_ttl", "%u", settings.ext_low_ttl);
    APPEND_STAT("ext_recache_rate", "%u", settings.ext_recache_rate);
    APPEND_STAT("ext_wbuf_size", "%u", settings.ext_wbuf_size);
    APPEND_STAT("ext_compact_under", "%u", settings.ext_compact_under);
    APPEND_STAT("ext_drop_under", "%u", settings.ext_drop_under);
    APPEND_STAT("ext_max_sleep", "%u", settings.ext_max_sleep);
    APPEND_STAT("ext_max_frag", "%.2f", settings.ext_max_frag);
    APPEND_STAT("slab_automove_freeratio", "%.3f", settings.slab_automove_freeratio);
    APPEND_STAT("ext_drop_unread", "%s", settings.ext_drop_unread ? "yes" : "no");
#endif
#ifdef TLS
    APPEND_STAT("ssl_enabled", "%s", settings.ssl_enabled ? "yes" : "no");
    APPEND_STAT("ssl_chain_cert", "%s", settings.ssl_chain_cert);
    APPEND_STAT("ssl_key", "%s", settings.ssl_key);
    APPEND_STAT("ssl_verify_mode", "%d", settings.ssl_verify_mode);
    APPEND_STAT("ssl_keyformat", "%d", settings.ssl_keyformat);
    APPEND_STAT("ssl_ciphers", "%s", settings.ssl_ciphers ? settings.ssl_ciphers : "NULL");
    APPEND_STAT("ssl_ca_cert", "%s", settings.ssl_ca_cert ? settings.ssl_ca_cert : "NULL");
    APPEND_STAT("ssl_wbuf_size", "%u", settings.ssl_wbuf_size);
    APPEND_STAT("ssl_session_cache", "%s", settings.ssl_session_cache ? "yes" : "no");
    APPEND_STAT("ssl_kernel_tls", "%s", settings.ssl_kernel_tls ? "yes" : "no");
    APPEND_STAT("ssl_min_version", "%s", ssl_proto_text(settings.ssl_min_version));
#endif
#ifdef PROXY
    APPEND_STAT("proxy_enabled", "%s", settings.proxy_enabled ? "yes" : "no");
    APPEND_STAT("proxy_uring_enabled", "%s", settings.proxy_uring ? "yes" : "no");
#endif
    APPEND_STAT("num_napi_ids", "%s", settings.num_napi_ids);
    APPEND_STAT("memory_file", "%s", settings.memory_file);
    APPEND_STAT("client_flags_size", "%d", sizeof(client_flags_t));
}

static int nz_strcmp(int nzlength, const char *nz, const char *z) {
    int zlength=strlen(z);
    return (zlength == nzlength) && (strncmp(nz, z, zlength) == 0) ? 0 : -1;
}

bool get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c) {
    bool ret = true;

    if (add_stats != NULL) {
        if (!stat_type) {
            /* prepare general statistics for the engine */
            STATS_LOCK();
            APPEND_STAT("bytes", "%llu", (unsigned long long)stats_state.curr_bytes);
            APPEND_STAT("curr_items", "%llu", (unsigned long long)stats_state.curr_items);
            APPEND_STAT("total_items", "%llu", (unsigned long long)stats.total_items);
            STATS_UNLOCK();
            APPEND_STAT("slab_global_page_pool", "%u", global_page_pool_size(NULL));
            item_stats_totals(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "items") == 0) {
            item_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "slabs") == 0) {
            slabs_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "sizes") == 0) {
            item_stats_sizes(add_stats, c);
        } else {
            ret = false;
        }
    } else {
        ret = false;
    }

    return ret;
}

static inline void get_conn_text(const conn *c, const int af,
                char* addr, struct sockaddr *sock_addr) {
    char addr_text[MAXPATHLEN];
    addr_text[0] = '\0';
    const char *protoname = "?";
    unsigned short port = 0;

    switch (af) {
        case AF_INET:
            (void) inet_ntop(af,
                    &((struct sockaddr_in *)sock_addr)->sin_addr,
                    addr_text,
                    sizeof(addr_text) - 1);
            port = ntohs(((struct sockaddr_in *)sock_addr)->sin_port);
            protoname = IS_UDP(c->transport) ? "udp" : "tcp";
            break;

        case AF_INET6:
            addr_text[0] = '[';
            addr_text[1] = '\0';
            if (inet_ntop(af,
                    &((struct sockaddr_in6 *)sock_addr)->sin6_addr,
                    addr_text + 1,
                    sizeof(addr_text) - 2)) {
                strncat(addr_text, "]", 2);
            }
            port = ntohs(((struct sockaddr_in6 *)sock_addr)->sin6_port);
            protoname = IS_UDP(c->transport) ? "udp6" : "tcp6";
            break;

#ifndef DISABLE_UNIX_SOCKET
        case AF_UNIX:
        {
            size_t pathlen = 0;
            // this strncpy call originally could piss off an address
            // sanitizer; we supplied the size of the dest buf as a limiter,
            // but optimized versions of strncpy could read past the end of
            // *src while looking for a null terminator. Since buf and
            // sun_path here are both on the stack they could even overlap,
            // which is "undefined". In all OSS versions of strncpy I could
            // find this has no effect; it'll still only copy until the first null
            // terminator is found. Thus it's possible to get the OS to
            // examine past the end of sun_path but it's unclear to me if this
            // can cause any actual problem.
            //
            // We need a safe_strncpy util function but I'll punt on figuring
            // that out for now.
            pathlen = sizeof(((struct sockaddr_un *)sock_addr)->sun_path);
            if (MAXPATHLEN <= pathlen) {
                pathlen = MAXPATHLEN - 1;
            }
            strncpy(addr_text,
                    ((struct sockaddr_un *)sock_addr)->sun_path,
                    pathlen);
            addr_text[pathlen] = '\0';
            protoname = "unix";
        }
            break;
#endif /* #ifndef DISABLE_UNIX_SOCKET */
    }

    if (strlen(addr_text) < 2) {
        /* Most likely this is a connected UNIX-domain client which
         * has no peer socket address, but there's no portable way
         * to tell for sure.
         */
        snprintf(addr_text, MAXPATHLEN, "<AF %d>", af);
    }

    if (port) {
        snprintf(addr, MAXPATHLEN + 11, "%s:%s:%u", protoname, addr_text, port);
    } else {
        snprintf(addr, MAXPATHLEN + 11, "%s:%s", protoname, addr_text);
    }
}

static void conn_to_str(const conn *c, char *addr, char *svr_addr) {
    if (!c) {
        memcpy(addr, "<null>", 6);
    } else if (c->state == conn_closed) {
        memcpy(addr, "<closed>", 8);
    } else {
        struct sockaddr_in6 local_addr;
        struct sockaddr *sock_addr = (void *)&c->request_addr;

        /* For listen ports and idle UDP ports, show listen address */
        if (c->state == conn_listening ||
                (IS_UDP(c->transport) &&
                 c->state == conn_read)) {
            memset(&local_addr, 0, sizeof(local_addr));
            socklen_t local_addr_len = sizeof(local_addr);

            if (getsockname(c->sfd,
                        (struct sockaddr *)&local_addr,
                        &local_addr_len) == 0) {
                sock_addr = (struct sockaddr *)&local_addr;
            }
        }
        get_conn_text(c, sock_addr->sa_family, addr, sock_addr);

        if (c->state != conn_listening && !(IS_UDP(c->transport) &&
                 c->state == conn_read)) {
            struct sockaddr_storage svr_sock_addr;
            memset(&svr_sock_addr, 0, sizeof(svr_sock_addr));
            socklen_t svr_addr_len = sizeof(svr_sock_addr);
            getsockname(c->sfd, (struct sockaddr *)&svr_sock_addr, &svr_addr_len);
            get_conn_text(c, svr_sock_addr.ss_family, svr_addr, (struct sockaddr *)&svr_sock_addr);
        }
    }
}

void process_stats_conns(ADD_STAT add_stats, void *c) {
    int i;
    char key_str[STAT_KEY_LEN];
    char val_str[STAT_VAL_LEN];
    size_t extras_len = sizeof(":unix:") + sizeof("65535");
    char addr[MAXPATHLEN + extras_len];
    char svr_addr[MAXPATHLEN + extras_len];
    memset(addr, 0, sizeof(addr));
    memset(svr_addr, 0, sizeof(svr_addr));
    int klen = 0, vlen = 0;

    assert(add_stats);

    for (i = 0; i < max_fds; i++) {
        if (conns[i]) {
            /* This is safe to do unlocked because conns are never freed; the
             * worst that'll happen will be a minor inconsistency in the
             * output -- not worth the complexity of the locking that'd be
             * required to prevent it.
             */
            if (IS_UDP(conns[i]->transport)) {
                APPEND_NUM_STAT(i, "UDP", "%s", "UDP");
            }
            if (conns[i]->state != conn_closed) {
                conn *sc = conns[i];
                conn_to_str(sc, addr, svr_addr);

                APPEND_NUM_STAT(i, "addr", "%s", addr);
                if (sc->state != conn_listening &&
                    !(IS_UDP(sc->transport) && sc->state == conn_read)) {
                    APPEND_NUM_STAT(i, "listen_addr", "%s", svr_addr);
                }
                APPEND_NUM_STAT(i, "state", "%s",
                        state_text(sc->state));
                if (sc->resps_suspended) {
                    APPEND_NUM_STAT(i, "resps_waiting", "%d", sc->resps_suspended);
                }
                APPEND_NUM_STAT(i, "secs_since_last_cmd", "%d",
                        current_time - sc->last_cmd_time);
            }
        }
    }
}

#define IT_REFCOUNT_LIMIT 60000
item* limited_get(const char *key, size_t nkey, LIBEVENT_THREAD *t, uint32_t exptime, bool should_touch, bool do_update, bool *overflow) {
    item *it;
    if (should_touch) {
        it = item_touch(key, nkey, exptime, t);
    } else {
        it = item_get(key, nkey, t, do_update); //여기서 thread.c -> item_get()으로 간다
    }
    if (it && it->refcount > IT_REFCOUNT_LIMIT) {
        item_remove(it);
        it = NULL;
        *overflow = true;
    } else {
        *overflow = false;
    }
    return it;
}

// Semantics are different than limited_get; since the item is returned
// locked, caller can directly change what it needs.
// though it might eventually be a better interface to sink it all into
// items.c.
item* limited_get_locked(const char *key, size_t nkey, LIBEVENT_THREAD *t, bool do_update, uint32_t *hv, bool *overflow) {
    item *it;
    it = item_get_locked(key, nkey, t, do_update, hv);
    if (it && it->refcount > IT_REFCOUNT_LIMIT) {
        do_item_remove(it);
        it = NULL;
        item_unlock(*hv);
        *overflow = true;
    } else {
        *overflow = false;
    }
    return it;
}

/*
 * adds a delta value to a numeric item.
 *
 * c     connection requesting the operation
 * it    item to adjust
 * incr  true to increment value, false to decrement
 * delta amount to adjust value by
 * buf   buffer for response string
 *
 * returns a response string to send back to the client.
 */
enum delta_result_type do_add_delta(LIBEVENT_THREAD *t, const char *key, const size_t nkey,
                                    const bool incr, const int64_t delta,
                                    char *buf, uint64_t *cas,
                                    const uint32_t hv,
                                    item **it_ret) {
    char *ptr;
    uint64_t value;
    int res;
    item *it;
    printf("memcached/memcached.c :: do_add_delta()\n");
    it = do_item_get(key, nkey, hv, t, DONT_UPDATE);
    if (!it) {
        return DELTA_ITEM_NOT_FOUND;
    }

    /* Can't delta zero byte values. 2-byte are the "\r\n" */
    /* Also can't delta for chunked items. Too large to be a number */
#ifdef EXTSTORE
    if (it->nbytes <= 2 || (it->it_flags & (ITEM_CHUNKED|ITEM_HDR)) != 0) {
#else
    if (it->nbytes <= 2 || (it->it_flags & (ITEM_CHUNKED)) != 0) {
#endif
        do_item_remove(it);
        return NON_NUMERIC;
    }

    if (cas != NULL && *cas != 0 && ITEM_get_cas(it) != *cas) {
        do_item_remove(it);
        return DELTA_ITEM_CAS_MISMATCH;
    }

    ptr = ITEM_data(it);

    if (!safe_strtoull(ptr, &value)) {
        do_item_remove(it);
        return NON_NUMERIC;
    }

    if (incr) {
        value += delta;
        //MEMCACHED_COMMAND_INCR(c->sfd, ITEM_key(it), it->nkey, value);
    } else {
        if(delta > value) {
            value = 0;
        } else {
            value -= delta;
        }
        //MEMCACHED_COMMAND_DECR(c->sfd, ITEM_key(it), it->nkey, value);
    }

    pthread_mutex_lock(&t->stats.mutex);
    if (incr) {
        t->stats.slab_stats[ITEM_clsid(it)].incr_hits++;
    } else {
        t->stats.slab_stats[ITEM_clsid(it)].decr_hits++;
    }
    pthread_mutex_unlock(&t->stats.mutex);

    itoa_u64(value, buf);
    res = strlen(buf);
    /* refcount == 2 means we are the only ones holding the item, and it is
     * linked. We hold the item's lock in this function, so refcount cannot
     * increase. */
    if (res + 2 <= it->nbytes && it->refcount == 2) { /* replace in-place */
        /* When changing the value without replacing the item, we
           need to update the CAS on the existing item. */
        /* We also need to fiddle it in the sizes tracker in case the tracking
         * was enabled at runtime, since it relies on the CAS value to know
         * whether to remove an item or not. */
        item_stats_sizes_remove(it);
        ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);
        item_stats_sizes_add(it);
        memcpy(ITEM_data(it), buf, res);
        memset(ITEM_data(it) + res, ' ', it->nbytes - res - 2);
        do_item_update(it);
    } else if (it->refcount > 1) {
        item *new_it;
        client_flags_t flags;
        FLAGS_CONV(it, flags);
        new_it = do_item_alloc(ITEM_key(it), it->nkey, flags, it->exptime, res + 2);
        if (new_it == 0) {
            do_item_remove(it);
            return EOM;
        }
        memcpy(ITEM_data(new_it), buf, res);
        memcpy(ITEM_data(new_it) + res, "\r\n", 2);
        item_replace(it, new_it, hv, (settings.use_cas) ? get_cas_id() : 0);
        // Overwrite the older item's CAS with our new CAS since we're
        // returning the CAS of the old item below.
        ITEM_set_cas(it, (settings.use_cas) ? ITEM_get_cas(new_it) : 0);
        do_item_remove(new_it);       /* release our reference */
    } else {
        /* Should never get here. This means we somehow fetched an unlinked
         * item. TODO: Add a counter? */
        if (settings.verbose) {
            fprintf(stderr, "Tried to do incr/decr on invalid item\n");
        }
        if (it->refcount == 1)
            do_item_remove(it);
        return DELTA_ITEM_NOT_FOUND;
    }

    if (cas) {
        *cas = ITEM_get_cas(it);    /* swap the incoming CAS value */
    }
    if (it_ret != NULL) {
        *it_ret = it;
    } else {
        do_item_remove(it);         /* release our reference */
    }
    return OK;
}

static int try_read_command_negotiate(conn *c) {
    assert(c != NULL);
    assert(c->protocol == negotiating_prot);
    assert(c->rcurr <= (c->rbuf + c->rsize));
    assert(c->rbytes > 0);
#if ENABLE_PRINT
        printf("memcached/memcached.c :: try_read_command_negotiate()\n");
#endif
    if ((unsigned char)c->rbuf[0] == (unsigned char)PROTOCOL_BINARY_REQ) {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: try_read_command_negotiate()1\n");
#endif
        c->protocol = binary_prot;
        c->try_read_command = try_read_command_binary;
    } else {
        // authentication doesn't work with negotiated protocol.
        c->protocol = ascii_prot;
#if ENABLE_PRINT
            printf("memcached/memcached.c :: try_read_command_negotiate()2\n");
#endif
        c->try_read_command = try_read_command_ascii;
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "%d: Client using the %s protocol\n", c->sfd, prot_text(c->protocol));
    }

    return c->try_read_command(c);
}

static int try_read_command_udp(conn *c) {
    assert(c != NULL);
    assert(c->rcurr <= (c->rbuf + c->rsize));
    assert(c->rbytes > 0);

    if ((unsigned char)c->rbuf[0] == (unsigned char)PROTOCOL_BINARY_REQ) {
        c->protocol = binary_prot;
        return try_read_command_binary(c);
    } else {
        c->protocol = ascii_prot;
        return try_read_command_ascii(c);
    }
}

/*
 * read a UDP request.
 */
static enum try_read_result try_read_udp(conn *c) {
    int res;

    assert(c != NULL);

    c->request_addr_size = sizeof(c->request_addr);
    res = recvfrom(c->sfd, c->rbuf, c->rsize,
                   0, (struct sockaddr *)&c->request_addr,
                   &c->request_addr_size);
    if (res > 8) {
        unsigned char *buf = (unsigned char *)c->rbuf;
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.bytes_read += res;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        /* Beginning of UDP packet is the request ID; save it. */
        c->request_id = buf[0] * 256 + buf[1];

        /* If this is a multi-packet request, drop it. */
        if (buf[4] != 0 || buf[5] != 1) {
            return READ_NO_DATA_RECEIVED;
        }

        /* Don't care about any of the rest of the header. */
        res -= 8;
        memmove(c->rbuf, c->rbuf + 8, res);

        c->rbytes = res;
        c->rcurr = c->rbuf;
        return READ_DATA_RECEIVED;
    }
    return READ_NO_DATA_RECEIVED;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 *
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start looking
 * at the data I've got after a number of reallocs...
 *
 * @return enum try_read_result
 */
static enum try_read_result try_read_network(conn *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: try_read_network()\n");
#endif
    enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
    int res;
    int num_allocs = 0;
    assert(c != NULL);

    if (c->rcurr != c->rbuf) {
        if (c->rbytes > 0) /* otherwise there's nothing to copy */
            memmove(c->rbuf, c->rcurr, c->rbytes); //rcurr의 위치에서 rbytes 만큼의 바이트를 rbuf로 옮기기
        c->rcurr = c->rbuf;
    }

    while (1) {
        // TODO: move to rbuf_* func?
        if (c->rbytes >= c->rsize && c->rbuf_malloced) {
            if (num_allocs == 4) {
                return gotdata;
            }
            ++num_allocs;
            char *new_rbuf = realloc(c->rbuf, c->rsize * 2);
            if (!new_rbuf) {
                STATS_LOCK();
                stats.malloc_fails++;
                STATS_UNLOCK();
                if (settings.verbose > 0) {
                    fprintf(stderr, "Couldn't realloc input buffer\n");
                }
                c->rbytes = 0; /* ignore what we read */
                out_of_memory(c, "SERVER_ERROR out of memory reading request");
                c->close_after_write = true;
                return READ_MEMORY_ERROR;
            }
            c->rcurr = c->rbuf = new_rbuf;
            c->rsize *= 2;
        }

        int avail = c->rsize - c->rbytes;

        #if ENABLE_SOCKET_FUNCTIONS
            res = c->read(c, c->rbuf + c->rbytes, avail);
        #endif

        if (res > 0) {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.bytes_read += res;
            pthread_mutex_unlock(&c->thread->stats.mutex);
            gotdata = READ_DATA_RECEIVED;
            c->rbytes += res;
            if (res == avail && c->rbuf_malloced) {
                // Resize rbuf and try a few times if huge ascii multiget.
                continue;
            } else {
                break;
            }
        }
        if (res == 0) {
            c->close_reason = NORMAL_CLOSE;
            return READ_ERROR;
        }
        if (res == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            return READ_ERROR;
        }
    }


    return gotdata;
}

static bool update_event(conn *c, const int new_flags) {
    assert(c != NULL);
#if ENABLE_PRINT
    printf("memcached/memcached.c :: update_event()\n");
#endif

    struct event_base *base = c->event.ev_base;
    if (c->ev_flags == new_flags)
        return true;
    if (event_del(&c->event) == -1) return false;
    event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = new_flags;

    if (event_add(&c->event, 0) == -1) return false;
    return true;
}

/*
 * Sets whether we are listening for new connections or not.
 */
void do_accept_new_conns(const bool do_accept) {
    conn *next;

    for (next = listen_conn; next; next = next->next) {
        if (do_accept) {
            update_event(next, EV_READ | EV_PERSIST);
            if (listen(next->sfd, settings.backlog) != 0) {
                perror("listen");
            }
        }
        else {
            update_event(next, 0);
            if (listen(next->sfd, 0) != 0) {
                perror("listen");
            }
        }
    }

    if (do_accept) {
        struct timeval maxconns_exited;
        uint64_t elapsed_us;
        gettimeofday(&maxconns_exited,NULL);
        STATS_LOCK();
        elapsed_us =
            (maxconns_exited.tv_sec - stats.maxconns_entered.tv_sec) * 1000000
            + (maxconns_exited.tv_usec - stats.maxconns_entered.tv_usec);
        stats.time_in_listen_disabled_us += elapsed_us;
        stats_state.accepting_conns = true;
        STATS_UNLOCK();
    } else {
        STATS_LOCK();
        stats_state.accepting_conns = false;
        gettimeofday(&stats.maxconns_entered,NULL);
        stats.listen_disabled_num++;
        STATS_UNLOCK();
        allow_new_conns = false;
        maxconns_handler(-42, 0, 0);
    }
}

#define TRANSMIT_ONE_RESP true
#define TRANSMIT_ALL_RESP false
static int _transmit_pre(conn *c, struct iovec *iovs, int iovused, bool one_resp) {
    mc_resp *resp = c->resp_head;
#if ENABLE_PRINT
    printf("memcached/memcached.c :: _transmit_pre() resp->wbuf : %s\n", resp->wbuf);
#endif

    while (resp && iovused + resp->iovcnt < IOV_MAX-1) {
#if ENABLE_PRINT
        printf("memcached/memcached.c :: _transmit_pre()1\n");
#endif
        if (resp->skip) {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: _transmit_pre()2\n");
#endif
            // Don't actually unchain the resp obj here since it's singly-linked.
            // Just let the post function handle it linearly.
            resp = resp->next;
            continue;
        }


        if (resp->chunked_data_iov) {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: _transmit_pre()3\n");
#endif
            // Handle chunked items specially.
            // They spend much more time in send so we can be a bit wasteful
            // in rebuilding iovecs for them.
            item_chunk *ch = (item_chunk *)ITEM_schunk((item *)resp->iov[resp->chunked_data_iov].iov_base);
            int x;
            for (x = 0; x < resp->iovcnt; x++) {
#if ENABLE_PRINT
                printf("memcached/memcached.c :: _transmit_pre()3-1\n");
#endif
                // This iov is tracking how far we've copied so far.
                if (x == resp->chunked_data_iov) {
                    int done = resp->chunked_total - resp->iov[x].iov_len;
                    // Start from the len to allow binprot to cut the \r\n
                    int todo = resp->iov[x].iov_len;
                    while (ch && todo > 0 && iovused < IOV_MAX-1) {
                        int skip = 0;
                        if (!ch->used) {
                            ch = ch->next;
                            continue;
                        }
                        // Skip parts we've already sent.
                        if (done >= ch->used) {
                            done -= ch->used;
                            ch = ch->next;
                            continue;
                        } else if (done) {
                            skip = done;
                            done = 0;
                        }
                        iovs[iovused].iov_base = ch->data + skip;
#if ENABLE_PRINT
                        printf("\t iovs[%d]->iov_base : %s\n",iovused, iovs[iovused].iov_base);
#endif
                        // Stupid binary protocol makes this go negative.
                        iovs[iovused].iov_len = ch->used - skip > todo ? todo : ch->used - skip;
                        iovused++;
                        todo -= ch->used - skip;
                        ch = ch->next;
                    }
                } else {
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: _transmit_pre()4\n");
#endif
                    iovs[iovused].iov_base = resp->iov[x].iov_base;
                    iovs[iovused].iov_len = resp->iov[x].iov_len;
                    iovused++;
                }
                if (iovused >= IOV_MAX-1){
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: _transmit_pre()5\n");
#endif
                    break;
                }
            }
        } else {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: _transmit_pre()5-1\n");
#endif
            //여기 들어감
            memcpy(&iovs[iovused], resp->iov, sizeof(struct iovec)*resp->iovcnt);
            iovused += resp->iovcnt;
        }
#if ENABLE_PRINT
        printf("memcached/memcached.c :: _transmit_pre()6\n");
#endif
        // done looking at first response, walk down the chain.
        resp = resp->next;
        //printf("\t resp->wbuf2 : %s\n", resp->wbuf);
        // used for UDP mode: UDP cannot send multiple responses per packet.
        if (one_resp)
            break;
    }
#if ENABLE_PRINT
    printf("memcached/memcached.c :: _transmit_pre()7, iovused : %d\n", iovused);
#endif
    return iovused;
}

/*
 * Decrements and completes responses based on how much data was transmitted.
 * Takes the connection and current result bytes.
 */
static void _transmit_post(conn *c, ssize_t res) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: _transmit_post() \n");
#endif
    // We've written some of the data. Remove the completed
    // responses from the list of pending writes.
    mc_resp *resp = c->resp_head;
#if ENABLE_PRINT
    printf("memcached/memcached.c :: _transmit_post() 2, resp -> wbuf : %s\n", resp->wbuf);
#endif
    while (resp) {
        int x;
        if (resp->skip) {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: _transmit_post() 3\n");
#endif
            resp = resp_finish(c, resp);
            continue;
        }

        // fastpath check. all small responses should cut here.
        if (res >= resp->tosend) { //여기 들어감
#if ENABLE_PRINT
            printf("memcached/memcached.c :: _transmit_post() 4, res : %d | resp->tosend : %d\n", res, resp->tosend);
#endif
            res -= resp->tosend;
            resp = resp_finish(c, resp);
            continue;
        }

        // it's fine to re-check iov's that were zeroed out before.
        for (x = 0; x < resp->iovcnt; x++) {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: _transmit_post() 5\n");
#endif
            struct iovec *iov = &resp->iov[x];
            if (res >= iov->iov_len) {
#if ENABLE_PRINT
                printf("memcached/memcached.c :: _transmit_post() 6\n");
#endif
                resp->tosend -= iov->iov_len;
                res -= iov->iov_len;
                iov->iov_len = 0;
            } else {
#if ENABLE_PRINT
                printf("memcached/memcached.c :: _transmit_post() 7\n");
#endif
                // Dumb special case for chunked items. Currently tracking
                // where to inject the chunked item via iov_base.
                // Extra not-great since chunked items can't be the first
                // index, so we have to check for non-zero c_d_iov first.
                if (!resp->chunked_data_iov || x != resp->chunked_data_iov) {
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: _transmit_post() 8\n");
#endif
                    iov->iov_base = (char *)iov->iov_base + res;
                }
                iov->iov_len -= res;
                resp->tosend -= res;
                res = 0;
                break;
            }
        }

        // are we done with this response object?
        if (resp->tosend == 0) {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: _transmit_post() 9\n");
#endif
            resp = resp_finish(c, resp);
        } else {
            // Jammed up here. This is the new head.
#if ENABLE_PRINT
            printf("memcached/memcached.c :: _transmit_post() 10\n");
#endif
            break;
        }
    }
#if ENABLE_PRINT
    printf("memcached/memcached.c :: _transmit_post() 11\n");
#endif
}

/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 msgbuf 구조체 리스트로부터 
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static enum transmit_result transmit(conn *c) {
    assert(c != NULL);
    struct iovec iovs[IOV_MAX]; //IOV_MAX : 1024
    struct msghdr msg; //[yedam] 메세지 헤더(나중에 sendmsg에 필요함)
    int iovused = 0;
#if ENABLE_PRINT
        printf("memcached/memcached.c :: transmit() 1\n");
#endif

    // init the msg.
    memset(&msg, 0, sizeof(struct msghdr));
    msg.msg_iov = iovs;

    iovused = _transmit_pre(c, iovs, iovused, TRANSMIT_ALL_RESP);
    if (iovused == 0) {
#if ENABLE_PRINT
        printf("memcached/memcached.c :: transmit, _transmit_pre 이후 iovused : %d\n", iovused);
#endif
        // Avoid the syscall if we're only handling a noreply.
        // Return the response object.
        _transmit_post(c, 0);
        return TRANSMIT_COMPLETE;
    }

    // Alright, send.
    ssize_t res;
    msg.msg_iovlen = iovused;
#if ENABLE_PRINT
        printf("memcached/memcached.c :: transmit() 2\n");
#endif
    
    res = c->sendmsg(c, &msg, 0);
    if (res >= 0) {
#if ENABLE_PRINT
        printf("memcached/memcached.c :: transmit(), res >= 0\n");
#endif
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.bytes_written += res;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        // Decrement any partial IOV's and complete any finished resp's.
        _transmit_post(c, res);

        if (c->resp_head) {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: transmit(), return TRANSMIT_INCOMPLETE\n");
#endif
            return TRANSMIT_INCOMPLETE;
        } else {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: transmit(), TRANSMIT_COMPLETE\n");
#endif
            return TRANSMIT_COMPLETE;
        }
    }

    if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (!update_event(c, EV_WRITE | EV_PERSIST)) {
            if (settings.verbose > 0)
                fprintf(stderr, "Couldn't update event\n");
            conn_set_state(c, conn_closing);
            return TRANSMIT_HARD_ERROR;
        }
        return TRANSMIT_SOFT_ERROR;
    }
    /* if res == -1 and error is not EAGAIN or EWOULDBLOCK,
       we have a real error, on which we close the connection */
    if (settings.verbose > 0)
        perror("Failed to write, and not due to blocking");

    conn_set_state(c, conn_closing);
    return TRANSMIT_HARD_ERROR;
}

static void build_udp_header(unsigned char *hdr, mc_resp *resp) {
    // We need to communicate the total number of packets
    // If this isn't set, it's the first time this response is building a udp
    // header, so "tosend" must be static.
    if (!resp->udp_total) {
        uint32_t total;
        total = resp->tosend / UDP_DATA_SIZE;
        if (resp->tosend % UDP_DATA_SIZE)
            total++;
        // The spec doesn't really say what we should do here. It's _probably_
        // better to bail out?
        if (total > USHRT_MAX) {
            total = USHRT_MAX;
        }
        resp->udp_total = total;
    }

    // TODO: why wasn't this hto*'s and casts?
    // this ends up sending UDP hdr data specifically in host byte order.
    *hdr++ = resp->request_id / 256;
    *hdr++ = resp->request_id % 256;
    *hdr++ = resp->udp_sequence / 256;
    *hdr++ = resp->udp_sequence % 256;
    *hdr++ = resp->udp_total / 256;
    *hdr++ = resp->udp_total % 256;
    *hdr++ = 0;
    *hdr++ = 0;
    resp->udp_sequence++;
}

/*
 * UDP specific transmit function. Uses its own function rather than check
 * IS_UDP() five times. If we ever implement sendmmsg or similar support they
 * will diverge even more.
 * Does not use TLS.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static enum transmit_result transmit_udp(conn *c) {
    assert(c != NULL);
    struct iovec iovs[IOV_MAX];
    struct msghdr msg;
    mc_resp *resp;
    int iovused = 0;
    unsigned char udp_hdr[UDP_HEADER_SIZE];

    // We only send one UDP packet per call (ugh), so we can only operate on a
    // single response at a time.
    resp = c->resp_head;

    if (!resp) {
        return TRANSMIT_COMPLETE;
    }

    if (resp->skip) {
        resp = resp_finish(c, resp);
        return TRANSMIT_INCOMPLETE;
    }

    // clear the message and initialize it.
    memset(&msg, 0, sizeof(struct msghdr));
    msg.msg_iov = iovs;

    // the UDP source to return to.
    msg.msg_name = &resp->request_addr;
    msg.msg_namelen = resp->request_addr_size;

    // First IOV is the custom UDP header.
    iovs[0].iov_base = (void *)udp_hdr;
    iovs[0].iov_len = UDP_HEADER_SIZE;
    build_udp_header(udp_hdr, resp);
    iovused++;

    // Fill the IOV's the standard way.
    // TODO: might get a small speedup if we let it break early with a length
    // limit.
    iovused = _transmit_pre(c, iovs, iovused, TRANSMIT_ONE_RESP);

    // Clip the IOV's to the max UDP packet size.
    // If we add support for send_mmsg, this can be where we split msg's.
    {
        int x = 0;
        int len = 0;
        for (x = 0; x < iovused; x++) {
            if (len + iovs[x].iov_len >= UDP_MAX_PAYLOAD_SIZE) {
                iovs[x].iov_len = UDP_MAX_PAYLOAD_SIZE - len;
                x++;
                break;
            } else {
                len += iovs[x].iov_len;
            }
        }
        iovused = x;
    }

    ssize_t res;
    msg.msg_iovlen = iovused;
    // NOTE: uses system sendmsg since we have no support for indirect UDP.
    res = sendmsg(c->sfd, &msg, 0);
    if (res >= 0) {
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.bytes_written += res;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        // Ignore the header size from forwarding the IOV's
        res -= UDP_HEADER_SIZE;

        // Decrement any partial IOV's and complete any finished resp's.
        _transmit_post(c, res);

        if (c->resp_head) {
            return TRANSMIT_INCOMPLETE;
        } else {
            return TRANSMIT_COMPLETE;
        }
    }

    if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (!update_event(c, EV_WRITE | EV_PERSIST)) {
            if (settings.verbose > 0)
                fprintf(stderr, "Couldn't update event\n");
            conn_set_state(c, conn_closing);
            return TRANSMIT_HARD_ERROR;
        }
        return TRANSMIT_SOFT_ERROR;
    }
    /* if res == -1 and error is not EAGAIN or EWOULDBLOCK,
       we have a real error, on which we close the connection */
    if (settings.verbose > 0)
        perror("Failed to write, and not due to blocking");

    conn_set_state(c, conn_read);
    return TRANSMIT_HARD_ERROR;
}


/* Does a looped read to fill data chunks */
/* TODO: restrict number of times this can loop.
 * Also, benchmark using readv's.
 */
static int read_into_chunked_item(conn *c) {
    int total = 0;
    int res;
    assert(c->rcurr != c->ritem);
#if ENABLE_PRINT
    printf("memcached/memcached.c :: read_into_chunked_item\n");
#endif
    while (c->rlbytes > 0) {
        item_chunk *ch = (item_chunk *)c->ritem;
        if (ch->size == ch->used) {
            // FIXME: ch->next is currently always 0. remove this?
            if (ch->next) {
                c->ritem = (char *) ch->next;
            } else {
                /* Allocate next chunk. Binary protocol needs 2b for \r\n */
                c->ritem = (char *) do_item_alloc_chunk(ch, c->rlbytes +
                       ((c->protocol == binary_prot) ? 2 : 0));
                if (!c->ritem) {
                    // We failed an allocation. Let caller handle cleanup.
                    total = -2;
                    break;
                }
                // ritem has new chunk, restart the loop.
                continue;
                //assert(c->rlbytes == 0);
            }
        }

        int unused = ch->size - ch->used;
        /* first check if we have leftovers in the conn_read buffer */
        if (c->rbytes > 0) {
            total = 0;
            int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
            tocopy = tocopy > unused ? unused : tocopy;
            if (c->ritem != c->rcurr) {
                memmove(ch->data + ch->used, c->rcurr, tocopy);
            }
            total += tocopy;
            c->rlbytes -= tocopy;
            c->rcurr += tocopy;
            c->rbytes -= tocopy;
            ch->used += tocopy;
            if (c->rlbytes == 0) {
                break;
            }
        } else {
            /*  now try reading from the socket */
            res = c->read(c, ch->data + ch->used,
                    (unused > c->rlbytes ? c->rlbytes : unused));
            if (res > 0) {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.bytes_read += res;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                ch->used += res;
                total += res;
                c->rlbytes -= res;
            } else {
                /* Reset total to the latest result so caller can handle it */
                total = res;
                break;
            }
        }
    }

    /* At some point I will be able to ditch the \r\n from item storage and
       remove all of these kludges.
       The above binprot check ensures inline space for \r\n, but if we do
       exactly enough allocs there will be no additional chunk for \r\n.
     */
    if (c->rlbytes == 0 && c->protocol == binary_prot && total >= 0) {
        item_chunk *ch = (item_chunk *)c->ritem;
        if (ch->size - ch->used < 2) {
            c->ritem = (char *) do_item_alloc_chunk(ch, 2);
            if (!c->ritem) {
                total = -2;
            }
        }
    }
    return total;
}

static void drive_machine(conn *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: drive_machine()\n");
#endif
    bool stop = false;
    int sfd;
    socklen_t addrlen;
    struct sockaddr_storage addr;
    int nreqs = settings.reqs_per_event;
    int res;
    const char *str;
#ifdef HAVE_ACCEPT4
    static int  use_accept4 = 1;
#else
    static int  use_accept4 = 0;
#endif

    assert(c != NULL);
    //printf("c->thread->open_bundle : %d\n", c->thread->open_bundle);
    while (!stop) {
        switch(c->state) {
        case conn_listening:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_listening\n");
#endif
            addrlen = sizeof(addr);
#ifdef HAVE_ACCEPT4
            if (use_accept4) { //여기 들어감
                sfd = accept4(c->sfd, (struct sockaddr *)&addr, &addrlen, SOCK_NONBLOCK);
            } else { //여기 안들어감
                sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen);
            }
#else
            sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen); //여기 안들어감
#endif
            if (sfd == -1) { //여기 안들어감
                if (use_accept4 && errno == ENOSYS) {
                    use_accept4 = 0;
                    continue;
                }
                perror(use_accept4 ? "accept4()" : "accept()");
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    /* these are transient, so don't log anything */
                    stop = true;
                } else if (errno == EMFILE) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Too many open connections\n");
                    accept_new_conns(false);
                    stop = true;
                } else {
                    perror("accept()");
                    stop = true;
                }
                break;
            }
            if (!use_accept4) { //여기 안들어감
                if (fcntl(sfd, F_SETFL, fcntl(sfd, F_GETFL) | O_NONBLOCK) < 0) {
                    perror("setting O_NONBLOCK");
                    close(sfd);
                    break;
                }
            }

            bool reject;
            if (settings.maxconns_fast) { //여기 들어감
                reject = sfd >= settings.maxconns - 1;
                if (reject) {
                    STATS_LOCK();
                    stats.rejected_conns++;
                    STATS_UNLOCK();
                }
            } else { //여기 안들어감
                reject = false;
            }

            if (reject) { //여기 안들어감
                str = "ERROR Too many open connections\r\n";
                res = write(sfd, str, strlen(str));
                close(sfd);
            } else { //여기 들어감
                // run accept routine if ssl is compiled + enabled
                bool fail = false;
                void *ssl_v = ssl_accept(c, sfd, &fail);
                if (fail) {
                    close(sfd);
                    break;
                }
                //여기 호출됨
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - call dispatch_conn_new\n");
#endif
                dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST,
                                     READ_BUFFER_CACHED, c->transport, ssl_v, c->tag, c->protocol);
            }
            stop = true;
            break;

        case conn_waiting:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_waiting\n");
#endif
            rbuf_release(c);
            if (!update_event(c, EV_READ | EV_PERSIST)) {
                if (settings.verbose > 0)
                    fprintf(stderr, "Couldn't update event\n");
                conn_set_state(c, conn_closing);
                break;
            }

            conn_set_state(c, conn_read);
            stop = true;
            break;

        case conn_read:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_read\n");
#endif
            if (!IS_UDP(c->transport)) { //여기로 들어감
                // Assign a read buffer if necessary.
                if (!rbuf_alloc(c)) { //여기는 안들어감
                    // TODO: Some way to allow for temporary failures.
                    conn_set_state(c, conn_closing);
                    break;
                }
                res = try_read_network(c); //여기 들어감
            } else {
                // UDP connections always have a static buffer.
                res = try_read_udp(c);
            }

            switch (res) {
            case READ_NO_DATA_RECEIVED: //여기 안들어감
                conn_set_state(c, conn_waiting);
                break;
            case READ_DATA_RECEIVED: //여기 들어감 -> conn_parse_cmd로 변경
                conn_set_state(c, conn_parse_cmd);
                break;
            case READ_ERROR: //여기 안들어감
                conn_set_state(c, conn_closing);
                break;
            case READ_MEMORY_ERROR: /* Failed to allocate more memory */ //여기 안들어감
                /* State already set by try_read_network */
                break;
            }
            break;

        case conn_parse_cmd:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_parse_cmd\n");    
#endif
            c->noreply = false;
            if (c->try_read_command(c) == 0) {
                /* we need more data! */
                if (c->resp_head) {
                    // Buffered responses waiting, flush in the meantime.
                    conn_set_state(c, conn_mwrite);
                } else {
                    conn_set_state(c, conn_waiting);
                }
            }

            break;

        case conn_new_cmd:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_new_cmd\n");
#endif
            /* Only process nreqs at a time to avoid starving other
               connections */
            printf("memcached/conn_new_cmd\n");
            --nreqs;
            if (nreqs >= 0) {
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_new_cmd1\n");
#endif
                reset_cmd_handler(c); //여기로 들어감
            } else if (c->resp_head) { //여기 안들어감
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_new_cmd2\n");
#endif
                // flush response pipe on yield.
                conn_set_state(c, conn_mwrite);
            } else { //여기 안들어감
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_new_cmd3\n");
#endif
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.conn_yields++;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                if (c->rbytes > 0) {
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: drive_machine - conn_new_cmd4\n");
#endif
                    /* We have already read in data into the input buffer,
                       so libevent will most likely not signal read events
                       on the socket (unless more data is available. As a
                       hack we should just put in a request to write data,
                       because that should be possible ;-)
                    */
                    if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                        if (settings.verbose > 0)
                            fprintf(stderr, "Couldn't update event\n");
                        conn_set_state(c, conn_closing);
                        break;
                    }
                }
                stop = true;
            }
            break;

        case conn_nread:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_nread\n");
#endif
            if (c->rlbytes == 0) {
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_nread, complete_nread\n");
#endif
                complete_nread(c);
                break;
            }

            /* Check if rbytes < 0, to prevent crash */
            if (c->rlbytes < 0) {
                if (settings.verbose) {
                    fprintf(stderr, "Invalid rlbytes to read: len %d\n", c->rlbytes);
                }
                conn_set_state(c, conn_closing);
                break;
            }

            if (c->item_malloced || ((((item *)c->item)->it_flags & ITEM_CHUNKED) == 0) ) {
                /* first check if we have leftovers in the conn_read buffer */
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_nread2\n");
#endif
                if (c->rbytes > 0) {
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: drive_machine - conn_nread3\n");
#endif
                    int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: drive_machine - conn_nread3, tocopy: %d\n", tocopy);
#endif
                    memmove(c->ritem, c->rcurr, tocopy);
                    c->ritem += tocopy;
                    c->rlbytes -= tocopy;
                    c->rcurr += tocopy;
                    c->rbytes -= tocopy;
                    if (c->rlbytes == 0) {
#if ENABLE_PRINT
                        printf("memcached/memcached.c :: drive_machine - conn_nread3-1\n");
#endif
                        break;
                    }
                }

                /*  now try reading from the socket */
                res = c->read(c, c->ritem, c->rlbytes);
                if (res > 0) {
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: drive_machine - conn_nread5\n");
#endif
                    pthread_mutex_lock(&c->thread->stats.mutex);
                    c->thread->stats.bytes_read += res;
                    pthread_mutex_unlock(&c->thread->stats.mutex);
                    if (c->rcurr == c->ritem) {
                        c->rcurr += res;
                    }
                    c->ritem += res;
                    c->rlbytes -= res;
                    break;
                }
            } else {
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: drive_machine - conn_nread6\n");
#endif
                res = read_into_chunked_item(c);
                if (res > 0)
                    break;
            }

            if (res == 0) { /* end of stream */
                c->close_reason = NORMAL_CLOSE;
                conn_set_state(c, conn_closing);
                break;
            }

            if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                stop = true;
                break;
            }

            /* Memory allocation failure */
            if (res == -2) {
                out_of_memory(c, "SERVER_ERROR Out of memory during read");
                c->sbytes = c->rlbytes;
                conn_set_state(c, conn_swallow);
                // Ensure this flag gets cleared. It gets killed on conn_new()
                // so any conn_closing is fine, calling complete_nread is
                // fine. This swallow semms to be the only other case.
                c->set_stale = false;
                c->mset_res = false;
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (settings.verbose > 0) {
                fprintf(stderr, "Failed to read, and not due to blocking:\n"
                        "errno: %d %s \n"
                        "rcurr=%p ritem=%p rbuf=%p rlbytes=%d rsize=%d\n",
                        errno, strerror(errno),
                        (void *)c->rcurr, (void *)c->ritem, (void *)c->rbuf,
                        (int)c->rlbytes, (int)c->rsize);
            }
            conn_set_state(c, conn_closing);
            break;

        case conn_swallow:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_swallow\n");
#endif
            /* we are reading sbytes and throwing them away */
            if (c->sbytes <= 0) {
                conn_set_state(c, conn_new_cmd);
                break;
            }

            /* first check if we have leftovers in the conn_read buffer */
            if (c->rbytes > 0) {
                int tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
                c->sbytes -= tocopy;
                c->rcurr += tocopy;
                c->rbytes -= tocopy;
                break;
            }

            /*  now try reading from the socket */
            res = c->read(c, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize);
            if (res > 0) {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.bytes_read += res;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                c->sbytes -= res;
                break;
            }
            if (res == 0) { /* end of stream */
                c->close_reason = NORMAL_CLOSE;
                conn_set_state(c, conn_closing);
                break;
            }
            if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                stop = true;
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (settings.verbose > 0)
                fprintf(stderr, "Failed to read, and not due to blocking\n");
            conn_set_state(c, conn_closing);
            break;

        case conn_write:
        case conn_mwrite:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_write, conn_mwrite\n");
#endif
            /* have side IO's that must process before transmit() can run.
             * remove the connection from the worker thread and dispatch the
             * IO queue
             transmit()이 실행되기 전에 먼저 처리해야 하는 부수적인 I/O 작업이 존재한다.
                해당 연결을 워커 스레드에서 제거하고, I/O 큐로 디스패치한다.
             */
            if (c->resps_suspended) {
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine, conn_mwrite1\n");
#endif
                LIBEVENT_THREAD *t = c->thread;
                // FIXME: carefully check or document somewhere that submit
                // cannot resume a connection in-line with submission.
                conn_set_state(c, conn_io_queue);
                if (t->conns_tosubmit++ > 20) {
                    // Run occasional batches, else submit outside event loop.
                    thread_io_queue_submit(c->thread);
                }

                stop = true;
                break;
            }
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine, conn_mwrite1\n");
#endif
            switch (!IS_UDP(c->transport) ? transmit(c) : transmit_udp(c)) { //요기 들어감
            case TRANSMIT_COMPLETE:
                if (c->state == conn_mwrite) { //요기 들어감
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: drive_machine, conn_mwrite2\n");
#endif

                    // Free up IO wraps and any half-uploaded items.
                    conn_release_items(c);
                    conn_set_state(c, conn_new_cmd);
                    if (c->close_after_write) {
                        conn_set_state(c, conn_closing);
                    }
                } else {
#if ENABLE_PRINT
                    printf("memcached/memcached.c :: drive_machine, conn_mwrite3\n");
#endif
                    if (settings.verbose > 0)
                        fprintf(stderr, "Unexpected state %d\n", c->state);
                    conn_set_state(c, conn_closing);
                }
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine, conn_mwrite4\n");
#endif
                break;

            case TRANSMIT_INCOMPLETE:
            case TRANSMIT_HARD_ERROR:
                break;                   /* Continue in state machine. */

            case TRANSMIT_SOFT_ERROR:
                stop = true;
                break;
            }
            break;

        case conn_closing:
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_closing\n");
#endif
            if (IS_UDP(c->transport))
                conn_cleanup(c);
            else
                conn_close(c);
            stop = true;
            break;

        case conn_closed:
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_closed\n");    
#endif
            /* This only happens if dormando is an idiot. */
            abort();
            break;

        case conn_watch:
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_watch\n");  
#endif
            /* We handed off our connection to the logger thread. */
            stop = true;
            break;
        case conn_io_queue:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_io_queue\n");   
#endif
            /* Woke up while waiting for an async return, but not ready. */
            event_del(&c->event);
            conn_set_state(c, conn_io_pending);
            stop = true;
            break;
        case conn_io_pending:
#if ENABLE_PRINT
            printf("memcached/memcached.c :: drive_machine - conn_io_pending\n");   
#endif
            /* Should not be reachable */
            assert(false);
            break;
        case conn_io_resume:
#if ENABLE_PRINT
                printf("memcached/memcached.c :: drive_machine - conn_io_resume\n");   
#endif
            /* Complete our queued IO's from within the worker thread. */
            conn_set_state(c, conn_mwrite);
            break;
        case conn_max_state:
            assert(false);
            break;
        }
    }

    return;
}

void event_handler(const evutil_socket_t fd, const short which, void *arg) {
#if ENABLE_PRINT
    printf("memcached/memcached.cc :: event_handler() [1] \n");
#endif
    conn *c;

    c = (conn *)arg;
    assert(c != NULL);

    c->which = which;

    /* sanity */
    if (fd != c->sfd) {
        if (settings.verbose > 0)
            fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
        conn_close(c);
        return;
    }
    if(c->state == conn_new_cmd){
#if ENABLE_PRINT
        printf("event hander c's state is conn_new_cmd\n");
        printf("memcached/memcached.c :: conn_new() c->thread->open_bundle : %d\n", c->thread->open_bundle);
#endif
    }
    drive_machine(c);

    /* wait for next event */
    return;
}

static int new_socket(struct addrinfo *ai) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: new_socket()-[1]\n");
#endif
    int sfd;
    int flags;
    //ai family : IVP6, ai_socktype : TCP connection, 
    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }
#if ENABLE_PRINT
    printf("\t std : %d\n", sfd);
#endif
    return sfd;
}


/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const int sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    /* Start with the default size. */
#ifdef _WIN32
    if (getsockopt((SOCKET)sfd, SOL_SOCKET, SO_SNDBUF, (char *)&old_size, &intsize) != 0) {
#else
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize) != 0) {
#endif /* #ifdef _WIN32 */
        if (settings.verbose > 0)
            perror("getsockopt(SO_SNDBUF)");
        return;
    }

    /* Binary-search for the real maximum. */
    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
}

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param port the port number to bind to
 * @param transport the transport protocol (TCP / UDP)
 * @param portnumber_file A filepointer to write the port numbers to
 *        when they are successfully added to the list of ports we
 *        listen on.

  * 소켓을 생성하고 특정 포트 번호에 바인딩합니다.
 * @param interface 바인딩할 네트워크 인터페이스
 * @param port 바인딩할 포트 번호
 * @param transport 전송 프로토콜 (TCP / UDP)
 * @param portnumber_file 포트가 정상적으로 리스닝 리스트에 추가되었을 때,  
 *                        해당 포트 번호를 기록할 파일 포인터
 */
static int server_socket(const char *interface, int port, enum network_transport transport, FILE *portnumber_file, uint8_t ssl_enabled, uint64_t conntag, enum protocol bproto) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: server_socket()-[1]\n");
#endif
    /*
    1. 여기서 주어지는 addrinfo 구조체의 변수를 가지고, 알기 쉬운 주소로 링크드 리스트 형태로 만들어줌(getaddrinfo)
    2. new socket을 통해 소켓 생성
    3. socket 객체에 특정 옵션들을 넣어준다.
    4. bind 진행
    5. listen 진행
    6. TCP니까, conn_new라고 하는 걸 통해 communication 객체를 생성해줌
    
    
    */
    int sfd;
    struct linger ling = {0, 0};
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints = { .ai_flags = AI_PASSIVE,
                              .ai_family = AF_UNSPEC };
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    int flags =1;

    hints.ai_socktype = IS_UDP(transport) ? SOCK_DGRAM : SOCK_STREAM; //SOCK_DGRAM : 2, SOCK_STREAM : 1
    //hinks.ai_socktype : 1이니까, TCP 통신을 사용한다는 의미
    //만약 2라면, SOCK_DGRAM이라서 UDP 통신을 사용한다는 의미가 된다
    
    if (port == -1) {
        port = 0;
    }
    snprintf(port_buf, sizeof(port_buf), "%d", port);
#if ENABLE_PRINT
    printf("\t port_buf : %s\n", port_buf);
    printf("\t interface : %s\n", interface);
#endif
    //여기 port_buf에는 내가 지정한 포트번호가 들어가있음

    //getaddrinfo는 netdb.h 헤더파일 안에 들어가있음
    //여기 함수 인자인 interface 와 port_buf를 IP주소/포트 등의 정보로 바꿔준다? 무슨의미인지 잘 모르겠다
    //interface : 114.16.136.217
    //port_buf : 10122
    //ai 안에는 결과 구조체의 포인터 주소가 담긴다.(연결에 사용할 주소 정보)
    //addrinfo라고 하는 구조체는 링크드리스트 형태로 되어있음.
    // 한 서버가 UDP/TCP/IPv4/IPv6 등등 여러 주소를 열 수도 있기 때문에 링크드 리스트로 ai_next가 있다!

    error= getaddrinfo(interface, port_buf, &hints, &ai);
#if ENABLE_PRINT
    printf("\t addrinfo ai member variables \n");
    printf("\t ai->ai_flags : %d \n", ai->ai_flags);
    printf("\t ai->family : %d \n", ai->ai_family);
    printf("\t ai->socktype : %d \n", ai->ai_socktype);
    printf("\t ai->protocol : %d \n", ai->ai_protocol);
    printf("\t ai->addrlen : %zu \n", ai->ai_addrlen);
    printf("\t ai->canonname : %d \n", ai->ai_canonname);
#endif



    if (error != 0) {
        if (error != EAI_SYSTEM)
          fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
        else
          perror("getaddrinfo()");
        return 1;
    }

    //next는 ai의 다음 연결된 주소 정보 구조체를 가지고 있으며,
    //next라고 하는 변수가 null(즉, 링크드 리스트에서 가장 끝까지)이 될 때까지 new_socket을 생성
    //근데 어차피 우리는 지금 IPv6?였나 TCP 프로토콜 한 주소만 쓰고있어서, 그냥 한번만 돎 ..
    for (next= ai; next; next= next->ai_next) { 
#if ENABLE_PRINT
        printf("memcached/memcached.c :: server_socket()-[2]\n");
#endif
        conn *listen_conn_add;
        if ((sfd = new_socket(next)) == -1) {// 오류가 생기진 않네 ..
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            if (errno == EMFILE) {
                /* ...unless we're out of fds */
                perror("server_socket");
                exit(EX_OSERR);
            }
            continue;
        }

        if (settings.num_napi_ids) { //여기는 안들어가네..
            ////printf("memcached/memcached.c :: server_socket()-[2]-3 settings.num_napi_ids\n");
            socklen_t len = sizeof(socklen_t);
            int napi_id;
            error = getsockopt(sfd, SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi_id, &len);
            if (error != 0) {
                fprintf(stderr, "-N <num_napi_ids> option not supported\n");
                exit(EXIT_FAILURE);
            }
        }

#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) { //IP 주소길이가 짱짱길다, AF_INET6은 IPv6용 주소 패밀리라고 함.
            //printf("memcached/memcached.c :: server_socket()-[2]-4 IPV6_V6ONLY\n");
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
                close(sfd);
                continue;
            }
        }
#endif
#ifdef SOCK_COOKIE_ID
        if (settings.sock_cookie_id != 0) { //여기 안들어감
            ////printf("memcached/memcached.c :: server_socket()-[2]-5 SOCK_COOKIE_ID\n");
            error = setsockopt(sfd, SOL_SOCKET, SOCK_COOKIE_ID, (void *)&settings.sock_cookie_id, sizeof(uint32_t));
            if (error != 0)
                perror("setsockopt");
        }
#endif
        //printf("memcached/memcached.c :: server_socket()-[3]\n");
        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        //이 setsockopt 녀석은 sys/socket.h 헤더파일에 있는 녀석.
        //소켓 파일 디스크립터에다가 특정 옵션을 설정할 때 사용하는 함수라고 한다(TCP_NODELAY 설정, SO_REUSEADDR 설정, 버퍼크기 조정 설정 등..)
        //여기서 SO_REUSEADDR은 포트 재사용 허용 설정임.
        //그럼 소켓 파일 디스크립터가 뭔데? OS가 소켓을 관리하기 위해 부여하는 숫자 ID
        if (IS_UDP(transport)) {
            maximize_sndbuf(sfd);
        } else {
            //여러 소켓 옵션 설정하는 코드
            //연결이 idle 상태가 되어도 주기적으로 메세지를 보내주는 옵션설정
            error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");
            //close() 호출 시, 남아있는 데이터를 다 보내고 종료할지, 아니면 걍 소켓을 바로 닫아버릴지 제어
            error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
            if (error != 0)
                perror("setsockopt");
            //nagle 알고리즘이라는 걸 비활성화해서, 작은 데이터도 지연없이 바로바로 전송되도록 함.
            error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");
        }

        //bind()함수로 소켓에다가 IP 주소와 포트 번호를 연결함
        //printf("memcached/memcached.c :: server_socket()-bind, sfd : %d\n", sfd);
        if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1) {
            if (errno != EADDRINUSE) {
                perror("bind()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            close(sfd);
            continue;
        } else {
            //printf("memcached/memcached.c :: server_socket()-bind success\n");
            success++;
            //socket을 수신 대기 상태로 바꿔주는 함수. 여기서부터는 클라이언트의 연결을 받을 준비가 되었다는 뜻
            if (!IS_UDP(transport) && listen(sfd, settings.backlog) == -1) {
                perror("listen()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }

            //printf("memcached/memcached.c :: server_socket()-portnumber_file : %d\n", portnumber_file);
            if (portnumber_file != NULL &&  //portnumber_file이 0이네 ..
                (next->ai_addr->sa_family == AF_INET ||
                 next->ai_addr->sa_family == AF_INET6)) {
                //printf("memcached/memcached.c :: server_socket()-bind success[2]\n");
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0) {
                    //printf("memcached/memcached.c :: server_socket()-bind success[3]\n");
                    if (next->ai_addr->sa_family == AF_INET) {
                        fprintf(portnumber_file, "%s INET: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in.sin_port));
                    } else {
                        //printf("memcached/memcached.c :: server_socket()-bind success[4]\n");
                        fprintf(portnumber_file, "%s INET6: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in6.sin6_port));
                    }
                }
            }
            //printf("memcached/memcached.c :: server_socket()-bind success[5]\n");
        }
        //printf("memcached/memcached.c :: server_socket()-[4]\n");
        if (IS_UDP(transport)) {
            int c;

            for (c = 0; c < settings.num_threads_per_udp; c++) {
                /* Allocate one UDP file descriptor per worker thread;
                 * this allows "stats conns" to separately list multiple
                 * parallel UDP requests in progress.
                 *
                 * The dispatch code round-robins new connection requests
                 * among threads, so this is guaranteed to assign one
                 * FD to each thread.
                 */
                int per_thread_fd;
                if (c == 0) {
                    per_thread_fd = sfd;
                } else {
                    per_thread_fd = dup(sfd);
                    if (per_thread_fd < 0) {
                        perror("Failed to duplicate file descriptor");
                        exit(EXIT_FAILURE);
                    }
                }
                dispatch_conn_new(per_thread_fd, conn_read,
                                  EV_READ | EV_PERSIST,
                                  UDP_READ_BUFFER_SIZE, transport, NULL, conntag, bproto);
            }
        } else { //TCP protocol
#if ENABLE_PRINT
            printf("memcached/memcached.c :: server_socket()-[5] sfd : %d, transport : %d\n", sfd, transport);
#endif
            if (!(listen_conn_add = conn_new(sfd, conn_listening,
                                             EV_READ | EV_PERSIST, 1,
                                             transport, main_base, NULL, conntag, bproto))) {
                fprintf(stderr, "failed to create listening connection\n");
                exit(EXIT_FAILURE);
            }
#ifdef TLS
            listen_conn_add->ssl_enabled = ssl_enabled;
#else
            assert(ssl_enabled == false);
#endif
            listen_conn_add->next = listen_conn; //전역변수 listen_conn, next는 conn 구조체들의 리스트를 생성하는 데 사용됨
            listen_conn = listen_conn_add;
        }
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    //printf("memcached/memcached.c :: server_socket()-[6], return\n");
    return success == 0;
}

static int server_sockets(int port, enum network_transport transport,
                          FILE *portnumber_file) {
    uint8_t ssl_enabled = MC_SSL_DISABLED;

    const char *notls = "notls";
    const char *btls = "btls";
    const char *mtls = "mtls";
    if (settings.ssl_enabled) {
        ssl_enabled = MC_SSL_ENABLED_DEFAULT;
    }
#if ENABLE_PRINT
    printf("memcached/memcached.c :: server_sockets()-[1]\n");
#endif

    if (settings.inter == NULL) { //여기 들어감
        //printf("memcached/memcached.c :: server_sockets()-[1]\n");
        return server_socket(settings.inter, port, transport, portnumber_file, ssl_enabled, 0, settings.binding_protocol);
    } else {
        // tokenize them and bind to each one of them..
        char *b;
        int ret = 0;
        char *list = strdup(settings.inter);

        if (list == NULL) {
            fprintf(stderr, "Failed to allocate memory for parsing server interface string\n");
            return 1;
        }
        // If we encounter any failure, preserve the first errno for the caller.
        int errno_save = 0;
        for (char *p = strtok_r(list, ";,", &b);
            p != NULL;
            p = strtok_r(NULL, ";,", &b)) {
            uint64_t conntag = 0;
            int the_port = port;
            if (settings.ssl_enabled) {
                ssl_enabled = MC_SSL_ENABLED_DEFAULT;
            }
            // "notls" option is valid only when memcached is run with SSL enabled.
            if (strncmp(p, notls, strlen(notls)) == 0) {
                if (!settings.ssl_enabled) {
                    fprintf(stderr, "'notls' option is valid only when SSL is enabled\n");
                    free(list);
                    return 1;
                }
                ssl_enabled = MC_SSL_DISABLED;
                p += strlen(notls) + 1;
            } else if (strncmp(p, btls, strlen(btls)) == 0) {
                 if (!settings.ssl_enabled) {
                    fprintf(stderr, "'btls' option is valid only when SSL is enabled\n");
                    free(list);
                    return 1;
                }
                ssl_enabled = MC_SSL_ENABLED_NOPEER;
                p += strlen(btls) + 1;
            } else if (strncmp(p, mtls, strlen(mtls)) == 0) {
                if (!settings.ssl_enabled) {
                    fprintf(stderr, "'otls' option is valid only when SSL is enabled\n");
                    free(list);
                    return 1;
                }
                ssl_enabled = MC_SSL_ENABLED_PEER;
                p += strlen(mtls) + 1;
            }

            // Allow forcing the protocol of this listener.
            const char *protostr = "proto";
            enum protocol bproto = settings.binding_protocol;
            if (strncmp(p, protostr, strlen(protostr)) == 0) {
                p += strlen(protostr);
                if (*p == '[') {
                    char *e = strchr(p, ']');
                    if (e == NULL) {
                        fprintf(stderr, "Invalid protocol spec: \"%s\"\n", p);
                        free(list);
                        return 1;
                    }
                    char *st = ++p; // skip '[';
                    *e = '\0';
                    size_t len = e - st;
                    p = ++e; // skip ']'
                    p++; // skip an assumed ':'

                    if (strncmp(st, "ascii", len) == 0) {
                        bproto = ascii_prot;
                    } else if (strncmp(st, "binary", len) == 0) {
                        bproto = binary_prot;
                    } else if (strncmp(st, "negotiating", len) == 0) {
                        bproto = negotiating_prot;
                    } else if (strncmp(st, "proxy", len) == 0) {
#ifdef PROXY
                        if (settings.proxy_enabled) {
                            bproto = proxy_prot;
                        } else {
                            fprintf(stderr, "Proxy must be enabled to use: \"%s\"\n", list);
                            free(list);
                            return 1;
                        }
#else
                        fprintf(stderr, "Server not built with proxy: \"%s\"\n", list);
                        free(list);
                        return 1;
#endif
                    }
                }
            }

            const char *tagstr = "tag";
            if (strncmp(p, tagstr, strlen(tagstr)) == 0) {
                p += strlen(tagstr);
                // NOTE: should probably retire the [] dumbassery. those're
                // shell characters.
                if (*p == '[' || *p == '_') {
                    char *e = strchr(p, ']');
                    if (e == NULL) {
                        e = strchr(p+1, '_');
                    }
                    if (e == NULL) {
                        fprintf(stderr, "Invalid tag in socket config: \"%s\"\n", p);
                        free(list);
                        return 1;
                    }
                    char *st = ++p; // skip '['
                    *e = '\0';
                    size_t len = e - st;
                    p = ++e; // skip ']'
                    p++; // skip an assumed ':'

                    // validate the tag and copy it in.
                    if (len > 8 || len < 1) {
                        fprintf(stderr, "Listener tags must be between 1 and 8 characters: \"%s\"\n", st);
                        free(list);
                        return 1;
                    }

                    // C programmers love turning string comparisons into
                    // integer comparisons.
                    memcpy(&conntag, st, len);
                }
            }

            char *h = NULL;
            if (*p == '[') {
                // expecting it to be an IPv6 address enclosed in []
                // i.e. RFC3986 style recommended by RFC5952
                char *e = strchr(p, ']');
                if (e == NULL) {
                    fprintf(stderr, "Invalid IPV6 address: \"%s\"", p);
                    free(list);
                    return 1;
                }
                h = ++p; // skip the opening '['
                *e = '\0';
                p = ++e; // skip the closing ']'
            }

            char *s = strchr(p, ':');
            if (s != NULL) {
                // If no more semicolons - attempt to treat as port number.
                // Otherwise the only valid option is an unenclosed IPv6 without port, until
                // of course there was an RFC3986 IPv6 address previously specified -
                // in such a case there is no good option, will just send it to fail as port number.
                if (strchr(s + 1, ':') == NULL || h != NULL) {
                    *s = '\0';
                    ++s;
                    if (!safe_strtol(s, &the_port)) {
                        fprintf(stderr, "Invalid port number: \"%s\"\n", s);
                        free(list);
                        return 1;
                    }
                }
            }

            if (h != NULL)
                p = h;

            if (strcmp(p, "*") == 0) {
                p = NULL;
            }
            //printf("memcached/memcached.c :: server_socket()-[2]\n");
            ret |= server_socket(p, the_port, transport, portnumber_file, ssl_enabled, conntag, bproto);
            if (ret != 0 && errno_save == 0) errno_save = errno;
        }
        free(list);
        errno = errno_save;
        return ret;
    }
}

#ifndef DISABLE_UNIX_SOCKET
static int new_socket_unix(void) {
    int sfd;
    int flags;

    if ((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket()");
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }
    return sfd;
}

static int server_socket_unix(const char *path, int access_mask) {
    int sfd;
    struct linger ling = {0, 0};
    struct sockaddr_un addr;
    struct stat tstat;
    int flags =1;
    int old_umask;

    if (!path) {
        return 1;
    }

    if ((sfd = new_socket_unix()) == -1) {
        return 1;
    }

    /*
     * Clean up a previous socket file if we left it around
     */
    if (lstat(path, &tstat) == 0) {
        if (S_ISSOCK(tstat.st_mode))
            unlink(path);
    }

    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

    /*
     * the memset call clears nonstandard fields in some implementations
     * that otherwise mess things up.
     */
    memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    assert(strcmp(addr.sun_path, path) == 0);
    old_umask = umask( ~(access_mask&0777));
    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("bind()");
        close(sfd);
        umask(old_umask);
        return 1;
    }
    umask(old_umask);
    if (listen(sfd, settings.backlog) == -1) {
        perror("listen()");
        close(sfd);
        return 1;
    }
#if ENABLE_PRINT
    printf("memcached/memcached.c :: server_socket_unix()-[1]\n"); //이거 진행 X
#endif
    if (!(listen_conn = conn_new(sfd, conn_listening,
                                 EV_READ | EV_PERSIST, 1,
                                 local_transport, main_base, NULL, 0, settings.binding_protocol))) {
        fprintf(stderr, "failed to create listening connection\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}
#else
#define server_socket_unix(path, access_mask)   -1
#endif /* #ifndef DISABLE_UNIX_SOCKET */

/*
 * We keep the current time of day in a global variable that's updated by a
 * timer event. This saves us a bunch of time() system calls (we really only
 * need to get the time once a second, whereas there can be tens of thousands
 * of requests a second) and allows us to use server-start-relative timestamps
 * rather than absolute UNIX timestamps, a space savings on systems where
 * sizeof(time_t) > sizeof(unsigned int).
 */
volatile rel_time_t current_time;
static struct event clockevent;
#ifdef MEMCACHED_DEBUG
volatile bool is_paused;
volatile int64_t delta;
#endif
#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
static bool monotonic = false;
static int64_t monotonic_start;
#endif

/* libevent uses a monotonic clock when available for event scheduling. Aside
 * from jitter, simply ticking our internal timer here is accurate enough.
 * Note that users who are setting explicit dates for expiration times *must*
 * ensure their clocks are correct before starting memcached. */
static void clock_handler(const evutil_socket_t fd, const short which, void *arg) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: clock_handler()\n");
#endif
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};
    static bool initialized = false;

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&clockevent);
    } else {
        initialized = true;
    }

    // While we're here, check for hash table expansion.
    // This function should be quick to avoid delaying the timer.
#if ENABLE_PRINT
    printf("memcached/memcached.c :: clock_handler() 1\n");
#endif
    assoc_start_expand(stats_state.curr_items);
    // also, if HUP'ed we need to do some maintenance.
    // for now that's just the authfile reload.
    if (settings.sig_hup) {
        settings.sig_hup = false;
#if ENABLE_PRINT
        printf("memcached/memcached.c :: clock_handler() 2\n");
#endif
        authfile_load(settings.auth_file);
#ifdef PROXY
        if (settings.proxy_ctx) {
            proxy_start_reload(settings.proxy_ctx);
        }
#endif
    }
#if ENABLE_PRINT
    printf("memcached/memcached.c :: clock_handler() 3\n");
#endif
    evtimer_set(&clockevent, clock_handler, 0);
    event_base_set(main_base, &clockevent);
    evtimer_add(&clockevent, &t);
#ifdef MEMCACHED_DEBUG
    if (is_paused) return;
#endif

#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
    if (monotonic) {
        struct timespec ts;
        if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1)
            return;
#ifdef MEMCACHED_DEBUG
        current_time = (rel_time_t) (ts.tv_sec - monotonic_start + delta);
#if ENABLE_PRINT
        printf("memcached/memcached.c :: clock_handler() - current_time 1 : %d\n", current_time);
#endif
#else
        current_time = (rel_time_t) (ts.tv_sec - monotonic_start);
#if ENABLE_PRINT
        printf("memcached/memcached.c :: clock_handler() - current_time 2 : %d\n", current_time);
#endif
#endif
        return;
    }
#endif
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
#ifdef MEMCACHED_DEBUG
        current_time = (rel_time_t) (tv.tv_sec - process_started + delta);
#if ENABLE_PRINT
        printf("memcached/memcached.c :: clock_handler() - current_time 3 : %d\n", current_time);
#endif
#else
        current_time = (rel_time_t) (tv.tv_sec - process_started);
#if ENABLE_PRINT
        printf("memcached/memcached.c :: clock_handler() - current_time 4 : %d\n", current_time);
#endif
#endif
    }
}

static const char* flag_enabled_disabled(bool flag) {
    return (flag ? "enabled" : "disabled");
}

void verify_default(const char* param, bool condition) {
    if (!condition) {
        printf("Default value of [%s] has changed."
            " Modify the help text and default value check.\n", param);
        exit(EXIT_FAILURE);
    }
}

static void usage(void) {
    printf(PACKAGE " " VERSION "\n");
    printf("-p, --port=<num>          TCP port to listen on (default: %d)\n"
           "-U, --udp-port=<num>      UDP port to listen on (default: %d, off)\n",
           settings.port, settings.udpport);
#ifndef DISABLE_UNIX_SOCKET
    printf("-s, --unix-socket=<file>  UNIX socket to listen on (disables network support)\n");
    printf("-a, --unix-mask=<mask>    access mask for UNIX socket, in octal (default: %o)\n",
            settings.access);
#endif /* #ifndef DISABLE_UNIX_SOCKET */
    printf("-A, --enable-shutdown     enable ascii \"shutdown\" command\n");
    printf("-l, --listen=<addr>       interface to listen on (default: INADDR_ANY)\n");
#ifdef TLS
    printf("                          if TLS/SSL is enabled, 'notls' prefix can be used to\n"
           "                          disable for specific listeners (-l notls:<ip>:<port>) \n");
#endif
    printf("-d, --daemon              run as a daemon\n"
           "-r, --enable-coredumps    maximize core file limit\n"
           "-u, --user=<user>         assume identity of <username> (only when run as root)\n"
           "-m, --memory-limit=<num>  item memory in megabytes (default: %lu)\n"
           "-M, --disable-evictions   return error on memory exhausted instead of evicting\n"
           "-c, --conn-limit=<num>    max simultaneous connections (default: %d)\n"
           "-k, --lock-memory         lock down all paged memory\n"
           "-v, --verbose             verbose (print errors/warnings while in event loop)\n"
           "-vv                       very verbose (also print client commands/responses)\n"
           "-vvv                      extremely verbose (internal state transitions)\n"
           "-h, --help                print this help and exit\n"
           "-i, --license             print memcached and libevent license\n"
           "-V, --version             print version and exit\n"
           "-P, --pidfile=<file>      save PID in <file>, only used with -d option\n"
           "-f, --slab-growth-factor=<num> chunk size growth factor (default: %2.2f)\n"
           "-n, --slab-min-size=<bytes> min space used for key+value+flags (default: %d)\n",
           (unsigned long) settings.maxbytes / (1 << 20),
           settings.maxconns, settings.factor, settings.chunk_size);
    verify_default("udp-port",settings.udpport == 0);
    printf("-L, --enable-largepages  try to use large memory pages (if available)\n");
    printf("-D <char>     Use <char> as the delimiter between key prefixes and IDs.\n"
           "              This is used for per-prefix stats reporting. The default is\n"
           "              \"%c\" (colon). If this option is specified, stats collection\n"
           "              is turned on automatically; if not, then it may be turned on\n"
           "              by sending the \"stats detail on\" command to the server.\n",
           settings.prefix_delimiter);
    printf("-t, --threads=<num>       number of threads to use (default: %d)\n", settings.num_threads);
    printf("-R, --max-reqs-per-event  maximum number of requests per event, limits the\n"
           "                          requests processed per connection to prevent \n"
           "                          starvation (default: %d)\n", settings.reqs_per_event);
    printf("-C, --disable-cas         disable use of CAS\n");
    printf("-b, --listen-backlog=<num> set the backlog queue limit (default: %d)\n", settings.backlog);
    printf("-B, --protocol=<name>     protocol - one of ascii, binary, or auto (default: %s)\n",
           prot_text(settings.binding_protocol));
    printf("-I, --max-item-size=<num> adjusts max item size\n"
           "                          (default: %dm, min: %dk, max: %dm)\n",
           settings.item_size_max/ (1 << 20), ITEM_SIZE_MAX_LOWER_LIMIT / (1 << 10),  ITEM_SIZE_MAX_UPPER_LIMIT / (1 << 20));
#ifdef ENABLE_SASL
    printf("-S, --enable-sasl         turn on Sasl authentication\n");
#endif
    printf("-F, --disable-flush-all   disable flush_all command\n");
    printf("-X, --disable-dumping     disable stats cachedump, stats detail, and lru_crawler metadump\n");
    printf("-W  --disable-watch       disable watch commands (live logging)\n");
    printf("-Y, --auth-file=<file>    (EXPERIMENTAL) enable ASCII protocol authentication. format:\n"
           "                          user:pass\\nuser2:pass2\\n\n");
    printf("-e, --memory-file=<file>  (EXPERIMENTAL) mmap a file for item memory.\n"
           "                          use only in ram disks or persistent memory mounts!\n"
           "                          enables restartable cache (stop with SIGUSR1)\n");
#ifdef TLS
    printf("-Z, --enable-ssl          enable TLS/SSL\n");
#endif
    printf("-o, --extended            comma separated list of extended options\n"
           "                          most options have a 'no_' prefix to disable\n"
           "   - maxconns_fast:       immediately close new connections after limit (default: %s)\n"
           "   - hashpower:           an integer multiplier for how large the hash\n"
           "                          table should be. normally grows at runtime. (default starts at: %d)\n"
           "                          set based on \"STAT hash_power_level\"\n"
           "   - tail_repair_time:    time in seconds for how long to wait before\n"
           "                          forcefully killing LRU tail item.\n"
           "                          disabled by default; very dangerous option.\n"
           "   - hash_algorithm:      the hash table algorithm\n"
           "                          default is murmur3 hash. options: jenkins, murmur3, xxh3\n"
           "   - no_lru_crawler:      disable LRU Crawler background thread.\n"
           "   - lru_crawler_sleep:   microseconds to sleep between items\n"
           "                          default is %d.\n"
           "   - lru_crawler_tocrawl: max items to crawl per slab per run\n"
           "                          default is %u (unlimited)\n",
           flag_enabled_disabled(settings.maxconns_fast), settings.hashpower_init,
           settings.lru_crawler_sleep, settings.lru_crawler_tocrawl);
    printf("   - read_buf_mem_limit:  limit in megabytes for connection read/response buffers.\n"
           "                          do not adjust unless you have high (20k+) conn. limits.\n"
           "                          0 means unlimited (default: %u)\n",
           settings.read_buf_mem_limit);
    verify_default("read_buf_mem_limit", settings.read_buf_mem_limit == 0);
    printf("   - no_lru_maintainer:   disable new LRU system + background thread.\n"
           "   - hot_lru_pct:         pct of slab memory to reserve for hot lru.\n"
           "                          (requires lru_maintainer, default pct: %d)\n"
           "   - warm_lru_pct:        pct of slab memory to reserve for warm lru.\n"
           "                          (requires lru_maintainer, default pct: %d)\n"
           "   - hot_max_factor:      items idle > cold lru age * drop from hot lru. (default: %.2f)\n"
           "   - warm_max_factor:     items idle > cold lru age * this drop from warm. (default: %.2f)\n"
           "   - temporary_ttl:       TTL's below get separate LRU, can't be evicted.\n"
           "                          (requires lru_maintainer, default: %d)\n"
           "   - idle_timeout:        timeout for idle connections. (default: %d, no timeout)\n",
           settings.hot_lru_pct, settings.warm_lru_pct, settings.hot_max_factor, settings.warm_max_factor,
           settings.temporary_ttl, settings.idle_timeout);
    printf("   - slab_chunk_max:      (EXPERIMENTAL) maximum slab size in kilobytes. use extreme care. (default: %d)\n"
           "   - watcher_logbuf_size: size in kilobytes of per-watcher write buffer. (default: %u)\n"
           "   - worker_logbuf_size:  size in kilobytes of per-worker-thread buffer\n"
           "                          read by background thread, then written to watchers. (default: %u)\n"
           "   - track_sizes:         enable dynamic reports for 'stats sizes' command.\n"
           "                          note that counts for each size are approximate.\n"
           "   - no_hashexpand:       disables hash table expansion (dangerous)\n"
           "   - modern:              enables options which will be default in future.\n"
           "                          currently: nothing\n"
           "   - no_modern:           uses defaults of previous major version (1.4.x)\n",
           settings.slab_chunk_size_max / (1 << 10), settings.logger_watcher_buf_size / (1 << 10),
           settings.logger_buf_size / (1 << 10));
    verify_default("tail_repair_time", settings.tail_repair_time == TAIL_REPAIR_TIME_DEFAULT);
    verify_default("lru_crawler_tocrawl", settings.lru_crawler_tocrawl == 0);
    verify_default("idle_timeout", settings.idle_timeout == 0);
#ifdef HAVE_DROP_PRIVILEGES
    printf("   - drop_privileges:     enable dropping extra syscall privileges\n"
           "   - no_drop_privileges:  disable drop_privileges in case it causes issues with\n"
           "                          some customisation.\n"
           "                          (default is no_drop_privileges)\n");
    verify_default("drop_privileges", !settings.drop_privileges);
#ifdef MEMCACHED_DEBUG
    printf("   - relaxed_privileges:  running tests requires extra privileges. (default: %s)\n",
           flag_enabled_disabled(settings.relaxed_privileges));
#endif
#endif
#ifdef SOCK_COOKIE_ID
    printf("   - sock_cookie_id:      attributes an ID to a socket for ip filtering/firewalls \n");
#endif
#ifdef EXTSTORE
    printf("\n   - External storage (ext_*) related options (see: https://memcached.org/extstore)\n");
    printf("   - ext_path:            file to write to for external storage.\n"
           "                          ie: ext_path=/mnt/d1/extstore:1G\n"
           "   - ext_page_size:       size in megabytes of storage pages. (default: %u)\n"
           "   - ext_wbuf_size:       size in megabytes of page write buffers. (default: %u)\n"
           "   - ext_threads:         number of IO threads to run. (default: %u)\n"
           "   - ext_item_size:       store items larger than this (bytes, default %u)\n"
           "   - ext_item_age:        store items idle at least this long (seconds, default: no age limit)\n"
           "   - ext_low_ttl:         consider TTLs lower than this specially (default: %u)\n"
           "   - ext_drop_unread:     don't re-write unread values during compaction (default: %s)\n"
           "   - ext_recache_rate:    recache an item every N accesses (default: %u)\n"
           "   - ext_compact_under:   compact when fewer than this many free pages\n"
           "                          (default: 1 percent of the assigned storage)\n"
           "   - ext_drop_under:      drop COLD items when fewer than this many free pages\n"
           "                          (default: 1/4th of the assigned storage)\n"
           "   - ext_max_frag:        only defrag pages if they are less full than this pct-wise (default: %.2f)\n"
           "   - ext_max_sleep:       max sleep time of background threads in us (default: %u)\n"
           "   - slab_automove_freeratio: ratio of memory to hold free as buffer.\n"
           "                          (see doc/storage.txt for more info, default: %.3f)\n",
           settings.ext_page_size / (1 << 20), settings.ext_wbuf_size / (1 << 20), settings.ext_io_threadcount,
           settings.ext_item_size, settings.ext_low_ttl,
           flag_enabled_disabled(settings.ext_drop_unread), settings.ext_recache_rate,
           settings.ext_max_frag, settings.ext_max_sleep, settings.slab_automove_freeratio);
    verify_default("ext_item_age", settings.ext_item_age == UINT_MAX);
#endif
#ifdef PROXY
    printf("   - proxy_config:        path to lua library file. separate with ':' for multiple files\n"
           "                          use proxy_config=routelib to use built-in library\n"
           "                          see https://memcached.org/proxy for information\n"
            );
    printf("   - proxy_arg:           argument string (file path) to pass to proxy config\n"
            );
#endif
    ssl_help();
    printf("-N, --napi_ids            number of napi ids. see doc/napi_ids.txt for more details\n");
    return;
}

static void usage_license(void) {
    printf(PACKAGE " " VERSION "\n\n");
    printf(
    "Copyright (c) 2003, Danga Interactive, Inc. <http://www.danga.com/>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions are\n"
    "met:\n"
    "\n"
    "    * Redistributions of source code must retain the above copyright\n"
    "notice, this list of conditions and the following disclaimer.\n"
    "\n"
    "    * Redistributions in binary form must reproduce the above\n"
    "copyright notice, this list of conditions and the following disclaimer\n"
    "in the documentation and/or other materials provided with the\n"
    "distribution.\n"
    "\n"
    "    * Neither the name of the Danga Interactive nor the names of its\n"
    "contributors may be used to endorse or promote products derived from\n"
    "this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
    "\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
    "LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
    "A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n"
    "OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n"
    "SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT\n"
    "LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"
    "OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    );

    return;
}

static void save_pid(const char *pid_file) {
    FILE *fp;
    if (access(pid_file, F_OK) == 0) {
        if ((fp = fopen(pid_file, "r")) != NULL) {
            char buffer[1024];
            if (fgets(buffer, sizeof(buffer), fp) != NULL) {
                unsigned int pid;
                if (safe_strtoul(buffer, &pid) && kill((pid_t)pid, 0) == 0) {
                    fprintf(stderr, "WARNING: The pid file contained the following (running) pid: %u\n", pid);
                }
            }
            fclose(fp);
        }
    }

    /* Create the pid file first with a temporary name, then
     * atomically move the file to the real name to avoid a race with
     * another process opening the file to read the pid, but finding
     * it empty.
     */
    char tmp_pid_file[1024];
    snprintf(tmp_pid_file, sizeof(tmp_pid_file), "%s.tmp", pid_file);

    if ((fp = fopen(tmp_pid_file, "w")) == NULL) {
        vperror("Could not open the pid file %s for writing", tmp_pid_file);
        return;
    }

    fprintf(fp,"%ld\n", (long)getpid());
    if (fclose(fp) == -1) {
        vperror("Could not close the pid file %s", tmp_pid_file);
    }

    if (rename(tmp_pid_file, pid_file) != 0) {
        vperror("Could not rename the pid file from %s to %s",
                tmp_pid_file, pid_file);
    }
}

static void remove_pidfile(const char *pid_file) {
  if (pid_file == NULL)
      return;

  if (unlink(pid_file) != 0) {
      vperror("Could not remove the pid file %s", pid_file);
  }

}

static void sig_handler(const int sig) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: sig_handler\n");
#endif
    stop_main_loop = EXIT_NORMALLY;
}

static void sighup_handler(const int sig) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: sighup_handler\n");
#endif
    settings.sig_hup = true;
}

static void sig_usrhandler(const int sig) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: sig_usrhandler\n");
#endif
    stop_main_loop = GRACE_STOP;
}

/*
 * On systems that supports multiple page sizes we may reduce the
 * number of TLB-misses by using the biggest available page size
 */
static int enable_large_pages(void) {
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
    int ret = -1;
    size_t sizes[32];
    int avail = getpagesizes(sizes, 32);
    if (avail != -1) {
        size_t max = sizes[0];
        struct memcntl_mha arg = {0};
        int ii;

        for (ii = 1; ii < avail; ++ii) {
            if (max < sizes[ii]) {
                max = sizes[ii];
            }
        }

        arg.mha_flags   = 0;
        arg.mha_pagesize = max;
        arg.mha_cmd = MHA_MAPSIZE_BSSBRK;

        if (memcntl(0, 0, MC_HAT_ADVISE, (caddr_t)&arg, 0, 0) == -1) {
            fprintf(stderr, "Failed to set large pages: %s\n",
                    strerror(errno));
            fprintf(stderr, "Will use default page size\n");
        } else {
            ret = 0;
        }
    } else {
        fprintf(stderr, "Failed to get supported pagesizes: %s\n",
                strerror(errno));
        fprintf(stderr, "Will use default page size\n");
    }

    return ret;
#elif defined(__linux__) && defined(MADV_HUGEPAGE)
    /* check if transparent hugepages is compiled into the kernel */
    /* RH based systems possibly uses a different path */
    static const char *mm_thp_paths[] = {
        "/sys/kernel/mm/transparent_hugepage/enabled",
        "/sys/kernel/mm/redhat_transparent_hugepage/enabled",
        NULL
    };

    char thpb[128] = {0};
    int pfd = -1;
    for (const char **p = mm_thp_paths; *p; p++) {
        if ((pfd = open(*p, O_RDONLY)) != -1)
            break;
    }

    if (pfd == -1) {
        fprintf(stderr, "Transparent huge pages support not detected.\n");
        fprintf(stderr, "Will use default page size.\n");
        return -1;
    }
    ssize_t rd = read(pfd, thpb, sizeof(thpb));
    close(pfd);
    if (rd <= 0) {
        fprintf(stderr, "Transparent huge pages could not read the configuration.\n");
        fprintf(stderr, "Will use default page size.\n");
        return -1;
    }
    thpb[rd] = 0;
    if (strstr(thpb, "[never]")) {
        fprintf(stderr, "Transparent huge pages support disabled.\n");
        fprintf(stderr, "Will use default page size.\n");
        return -1;
    }
    return 0;
#elif defined(__FreeBSD__)
    int spages;
    size_t spagesl = sizeof(spages);

    if (sysctlbyname("vm.pmap.pg_ps_enabled", &spages,
    &spagesl, NULL, 0) != 0) {
        fprintf(stderr, "Could not evaluate the presence of superpages features.");
        return -1;
    }
    if (spages != 1) {
        fprintf(stderr, "Superpages support not detected.\n");
        fprintf(stderr, "Will use default page size.\n");
        return -1;
    }
    return 0;
#else
    return -1;
#endif
}

/**
 * Do basic sanity check of the runtime environment
 * @return true if no errors found, false if we can't use this env
 */
static bool sanitycheck(void) {
    /* One of our biggest problems is old and bogus libevents */
    const char *ever = event_get_version();
    if (ever != NULL) {
        if (strncmp(ever, "1.", 2) == 0) {
            fprintf(stderr, "You are using libevent %s.\nPlease upgrade to 2.x"
                        " or newer\n", event_get_version());
            return false;
        }
    }

    return true;
}

static bool _parse_slab_sizes(char *s, uint32_t *slab_sizes) {
    char *b = NULL;
    uint32_t size = 0;
    int i = 0;
    uint32_t last_size = 0;

    if (strlen(s) < 1)
        return false;

    for (char *p = strtok_r(s, "-", &b);
         p != NULL;
         p = strtok_r(NULL, "-", &b)) {
        if (!safe_strtoul(p, &size) || size < settings.chunk_size
             || size > settings.slab_chunk_size_max) {
            fprintf(stderr, "slab size %u is out of valid range\n", size);
            return false;
        }
        if (last_size >= size) {
            fprintf(stderr, "slab size %u cannot be lower than or equal to a previous class size\n", size);
            return false;
        }
        if (size <= last_size + CHUNK_ALIGN_BYTES) {
            fprintf(stderr, "slab size %u must be at least %d bytes larger than previous class\n",
                    size, CHUNK_ALIGN_BYTES);
            return false;
        }
        slab_sizes[i++] = size;
        last_size = size;
        if (i >= MAX_NUMBER_OF_SLAB_CLASSES-1) {
            fprintf(stderr, "too many slab classes specified\n");
            return false;
        }
    }

    slab_sizes[i] = 0;
    return true;
}

struct _mc_meta_data {
    void *mmap_base;
    uint64_t old_base;
    char *slab_config; // string containing either factor or custom slab list.
    int64_t time_delta;
    uint64_t process_started;
    uint32_t current_time;
};

// We need to remember a combination of configuration settings and global
// state for restart viability and resumption of internal services.
// Compared to the number of tunables and state values, relatively little
// does need to be remembered.
// Time is the hardest; we have to assume the sys clock is correct and re-sync for
// the lost time after restart.
static int _mc_meta_save_cb(const char *tag, void *ctx, void *data) {
    struct _mc_meta_data *meta = (struct _mc_meta_data *)data;

    // Settings to remember.
    // TODO: should get a version of version which is numeric, else
    // comparisons for compat reasons are difficult.
    // it may be possible to punt on this for now; since we can test for the
    // absence of another key... such as the new numeric version.
    //restart_set_kv(ctx, "version", "%s", VERSION);
    // We hold the original factor or subopts _string_
    // it can be directly compared without roundtripping through floats or
    // serializing/deserializing the long options list.
    restart_set_kv(ctx, "slab_config", "%s", meta->slab_config);
    restart_set_kv(ctx, "maxbytes", "%llu", (unsigned long long) settings.maxbytes);
    restart_set_kv(ctx, "chunk_size", "%d", settings.chunk_size);
    restart_set_kv(ctx, "item_size_max", "%d", settings.item_size_max);
    restart_set_kv(ctx, "slab_chunk_size_max", "%d", settings.slab_chunk_size_max);
    restart_set_kv(ctx, "slab_page_size", "%d", settings.slab_page_size);
    restart_set_kv(ctx, "use_cas", "%s", settings.use_cas ? "true" : "false");
    restart_set_kv(ctx, "slab_reassign", "%s", settings.slab_reassign ? "true" : "false");

    // Online state to remember.

    // current time is tough. we need to rely on the clock being correct to
    // pull the delta between stop and start times. we also need to know the
    // delta between start time and now to restore monotonic clocks.
    // for non-monotonic clocks (some OS?), process_started is the only
    // important one.
    restart_set_kv(ctx, "current_time", "%u", current_time);
    // types are great until... this. some systems time_t could be big, but
    // I'm assuming never negative.
    restart_set_kv(ctx, "process_started", "%llu", (unsigned long long) process_started);
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        restart_set_kv(ctx, "stop_time", "%lu", tv.tv_sec);
    }

    // Might as well just fetch the next CAS value to use than tightly
    // coupling the internal variable into the restart system.
    restart_set_kv(ctx, "current_cas", "%llu", (unsigned long long) get_cas_id());
    restart_set_kv(ctx, "logger_gid", "%llu", logger_get_gid());
    restart_set_kv(ctx, "hashpower", "%u", stats_state.hash_power_level);
    // NOTE: oldest_live is a rel_time_t, which aliases for unsigned int.
    // should future proof this with a 64bit upcast, or fetch value from a
    // converter function/macro?
    printf("memcached.c::_mc_meta_save_cb(), oldest_live \n");
    restart_set_kv(ctx, "oldest_live", "%u", settings.oldest_live);
    // TODO: use uintptr_t etc? is it portable enough?
    restart_set_kv(ctx, "mmap_oldbase", "%p", meta->mmap_base);

    return 0;
}

// We must see at least this number of checked lines. Else empty/missing lines
// could cause a false-positive.
// TODO: Once crc32'ing of the metadata file is done this could be ensured better by
// the restart module itself (crc32 + count of lines must match on the
// backend)
#define RESTART_REQUIRED_META 16

// With this callback we make a decision on if the current configuration
// matches up enough to allow reusing the cache.
// We also re-load important runtime information.
static int _mc_meta_load_cb(const char *tag, void *ctx, void *data) {
    struct _mc_meta_data *meta = (struct _mc_meta_data *)data;
    char *key;
    char *val;
    int reuse_mmap = 0;
    meta->process_started = 0;
    meta->time_delta = 0;
    meta->current_time = 0;
    int lines_seen = 0;

    // TODO: not sure this is any better than just doing an if/else tree with
    // strcmp's...
    enum {
        R_MMAP_OLDBASE = 0,
        R_MAXBYTES,
        R_CHUNK_SIZE,
        R_ITEM_SIZE_MAX,
        R_SLAB_CHUNK_SIZE_MAX,
        R_SLAB_PAGE_SIZE,
        R_SLAB_CONFIG,
        R_USE_CAS,
        R_SLAB_REASSIGN,
        R_CURRENT_CAS,
        R_OLDEST_LIVE,
        R_LOGGER_GID,
        R_CURRENT_TIME,
        R_STOP_TIME,
        R_PROCESS_STARTED,
        R_HASHPOWER,
    };

    const char *opts[] = {
        [R_MMAP_OLDBASE] = "mmap_oldbase",
        [R_MAXBYTES] = "maxbytes",
        [R_CHUNK_SIZE] = "chunk_size",
        [R_ITEM_SIZE_MAX] = "item_size_max",
        [R_SLAB_CHUNK_SIZE_MAX] = "slab_chunk_size_max",
        [R_SLAB_PAGE_SIZE] = "slab_page_size",
        [R_SLAB_CONFIG] = "slab_config",
        [R_USE_CAS] = "use_cas",
        [R_SLAB_REASSIGN] = "slab_reassign",
        [R_CURRENT_CAS] = "current_cas",
        [R_OLDEST_LIVE] = "oldest_live",
        [R_LOGGER_GID] = "logger_gid",
        [R_CURRENT_TIME] = "current_time",
        [R_STOP_TIME] = "stop_time",
        [R_PROCESS_STARTED] = "process_started",
        [R_HASHPOWER] = "hashpower",
        NULL
    };

    while (restart_get_kv(ctx, &key, &val) == RESTART_OK) {
        int type = 0;
        int32_t val_int = 0;
        uint32_t val_uint = 0;
        int64_t bigval_int = 0;
        uint64_t bigval_uint = 0;

        while (opts[type] != NULL && strcmp(key, opts[type]) != 0) {
            type++;
        }
        if (opts[type] == NULL) {
            fprintf(stderr, "[restart] unknown/unhandled key: %s\n", key);
            continue;
        }
        lines_seen++;

        // helper for any boolean checkers.
        bool val_bool = false;
        bool is_bool = true;
        if (strcmp(val, "false") == 0) {
            val_bool = false;
        } else if (strcmp(val, "true") == 0) {
            val_bool = true;
        } else {
            is_bool = false;
        }

        switch (type) {
        case R_MMAP_OLDBASE:
            if (!safe_strtoull_hex(val, &meta->old_base)) {
                fprintf(stderr, "[restart] failed to parse %s: %s\n", key, val);
                reuse_mmap = -1;
            }
            break;
        case R_MAXBYTES:
            if (!safe_strtoll(val, &bigval_int) || settings.maxbytes != bigval_int) {
                reuse_mmap = -1;
            }
            break;
        case R_CHUNK_SIZE:
            if (!safe_strtol(val, &val_int) || settings.chunk_size != val_int) {
                reuse_mmap = -1;
            }
            break;
        case R_ITEM_SIZE_MAX:
            if (!safe_strtol(val, &val_int) || settings.item_size_max != val_int) {
                reuse_mmap = -1;
            }
            break;
        case R_SLAB_CHUNK_SIZE_MAX:
            if (!safe_strtol(val, &val_int) || settings.slab_chunk_size_max != val_int) {
                reuse_mmap = -1;
            }
            break;
        case R_SLAB_PAGE_SIZE:
            if (!safe_strtol(val, &val_int) || settings.slab_page_size != val_int) {
                reuse_mmap = -1;
            }
            break;
        case R_SLAB_CONFIG:
            if (strcmp(val, meta->slab_config) != 0) {
                reuse_mmap = -1;
            }
            break;
        case R_USE_CAS:
            if (!is_bool || settings.use_cas != val_bool) {
                reuse_mmap = -1;
            }
            break;
        case R_SLAB_REASSIGN:
            if (!is_bool || settings.slab_reassign != val_bool) {
                reuse_mmap = -1;
            }
            break;
        case R_CURRENT_CAS:
            // FIXME: do we need to fail if these values _aren't_ found?
            if (!safe_strtoull(val, &bigval_uint)) {
                reuse_mmap = -1;
            } else {
                set_cas_id(bigval_uint);
            }
            break;
        case R_OLDEST_LIVE:
            if (!safe_strtoul(val, &val_uint)) {
                reuse_mmap = -1;
            } else {
                printf("memcached.c::_mc_meta_load_cb(), settings.oldest_live = val_uint\n", settings.oldest_live);
                settings.oldest_live = val_uint;
            }
            break;
        case R_LOGGER_GID:
            if (!safe_strtoull(val, &bigval_uint)) {
                reuse_mmap = -1;
            } else {
                logger_set_gid(bigval_uint);
            }
            break;
        case R_PROCESS_STARTED:
            if (!safe_strtoull(val, &bigval_uint)) {
                reuse_mmap = -1;
            } else {
                meta->process_started = bigval_uint;
            }
            break;
        case R_CURRENT_TIME:
            if (!safe_strtoul(val, &val_uint)) {
                reuse_mmap = -1;
            } else {
                meta->current_time = val_uint;
            }
            break;
        case R_STOP_TIME:
            if (!safe_strtoll(val, &bigval_int)) {
                reuse_mmap = -1;
            } else {
                struct timeval t;
                gettimeofday(&t, NULL);
                meta->time_delta = t.tv_sec - bigval_int;
                // clock has done something crazy.
                // there are _lots_ of ways the clock can go wrong here, but
                // this is a safe sanity check since there's nothing else we
                // can realistically do.
                if (meta->time_delta <= 0) {
                    reuse_mmap = -1;
                }
            }
            break;
        case R_HASHPOWER:
            if (!safe_strtoul(val, &val_uint)) {
                reuse_mmap = -1;
            } else {
                settings.hashpower_init = val_uint;
            }
            break;
        default:
            fprintf(stderr, "[restart] unhandled key: %s\n", key);
        }

        if (reuse_mmap != 0) {
            fprintf(stderr, "[restart] restart incompatible due to setting for [%s] [old value: %s]\n", key, val);
            break;
        }
    }

    if (lines_seen < RESTART_REQUIRED_META) {
        fprintf(stderr, "[restart] missing some metadata lines\n");
        reuse_mmap = -1;
    }

    return reuse_mmap;
}


#if ENABLE_MPI_FUNCTIONS

bool mpi_rbuf_switch_to_malloc(struct mpi_conn_info *c) {
    // Might as well start with x2 and work from there.
    size_t size = c->rsize * 2;
    char *tmp = malloc(size);
    if (!tmp)
        return false;

    memcpy(tmp, c->rcurr, c->rbytes);
    do_cache_free(c->thread->rbuf_cache, c->rbuf);

    c->rcurr = c->rbuf = tmp;
    c->rsize = size;
    c->rbuf_malloced = true;
    return true;
}



static int mpi_read_into_chunked_item(struct mpi_conn_info *c) {
    int total = 0;
    int res;
    assert(c->rcurr != c->ritem);
#if ENABLE_PRINT
    printf("memcached/memcached.c :: read_into_chunked_item\n");
#endif
    while (c->rlbytes > 0) {
        item_chunk *ch = (item_chunk *)c->ritem;
        if (ch->size == ch->used) {
            // FIXME: ch->next is currently always 0. remove this?
            if (ch->next) {
                c->ritem = (char *) ch->next;
            } else {
                /* Allocate next chunk. Binary protocol needs 2b for \r\n */
                c->ritem = (char *) do_item_alloc_chunk(ch, c->rlbytes +
                       ((c->protocol == binary_prot) ? 2 : 0));
                if (!c->ritem) {
                    // We failed an allocation. Let caller handle cleanup.
                    total = -2;
                    break;
                }
                // ritem has new chunk, restart the loop.
                continue;
                //assert(c->rlbytes == 0);
            }
        }

        int unused = ch->size - ch->used;
        /* first check if we have leftovers in the conn_read buffer */
        if (c->rbytes > 0) {
            total = 0;
            int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
            tocopy = tocopy > unused ? unused : tocopy;
            if (c->ritem != c->rcurr) {
                memmove(ch->data + ch->used, c->rcurr, tocopy);
            }
            total += tocopy;
            c->rlbytes -= tocopy;
            c->rcurr += tocopy;
            c->rbytes -= tocopy;
            ch->used += tocopy;
            if (c->rlbytes == 0) {
                break;
            }
        } else {
            /*  now try reading from the socket */
            //res = c->read(c, ch->data + ch->used, (unused > c->rlbytes ? c->rlbytes : unused));
            res = mpi_try_read_network(c);
            if (res > 0) {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.bytes_read += res;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                ch->used += res;
                total += res;
                c->rlbytes -= res;
            } else {
                /* Reset total to the latest result so caller can handle it */
                total = res;
                break;
            }
        }
    }

    /* At some point I will be able to ditch the \r\n from item storage and
       remove all of these kludges.
       The above binprot check ensures inline space for \r\n, but if we do
       exactly enough allocs there will be no additional chunk for \r\n.
     */
    if (c->rlbytes == 0 && c->protocol == binary_prot && total >= 0) {
        item_chunk *ch = (item_chunk *)c->ritem;
        if (ch->size - ch->used < 2) {
            c->ritem = (char *) do_item_alloc_chunk(ch, 2);
            if (!c->ritem) {
                total = -2;
            }
        }
    }
    return total;
}

void mpi_conn_release_items(struct mpi_conn_info *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_conn_release_items ()\n");
#endif
    assert(c != NULL);

    if (c->item) {
        if (c->item_malloced) {
            free(c->item);
            c->item_malloced = false;
        } else {
            item_remove(c->item);
        }
        c->item = 0;
    }

    // Cull any unsent responses.
    if (c->resp_head) {
        mc_resp *resp = c->resp_head;
        // r_f() handles the chain maintenance.
        while (resp) {
            // temporary by default. hide behind a debug flag in the future:
            // double free detection. Transmit loops can drop out early, but
            // here we could infinite loop.
            if (resp->free) {
                fprintf(stderr, "ERROR: double free detected during conn_release_items(): [%d] [%s]\n",
                        c->rank, c->protocol == binary_prot ? "binary" : "ascii");
                // Since this is a critical failure, just leak the memory.
                // If these errors are seen, an abort() can be used instead.
                c->resp_head = NULL;
                c->resp = NULL;
                break;
            }
            resp = resp_finish(c, resp);
        }
    }
}

void mpi_conn_init(struct mpi_conn_info* mpi_conn_arr, int size){
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_conn_init()\n");
#endif
    int server_size;
#if MEMCACHED_MODE
    server_size = size/2;
    mpi_client_size = size/2;
#endif

#if PS_MODE
    server_size = size/2;
#endif

    for(int i=0; i<server_size; i++){
        #if MEMCACHED_MODE
            mpi_conn_arr[i].rank = 1;
        #endif
        #if PS_MODE
            mpi_conn_arr[i].rank = i+server_size+1;
        #endif
        mpi_conn_arr[i].state = conn_listening;
        mpi_conn_arr[i].rsize = READ_BUFFER_SIZE; //16kb
        mpi_conn_arr[i].rbuf = NULL;
        mpi_conn_arr[i].rcurr = mpi_conn_arr[i].rbuf;
        mpi_conn_arr[i].rbytes = 0;
        mpi_conn_arr[i].thread = calloc(1, sizeof(LIBEVENT_THREAD));
        mpi_conn_arr[i].item = NULL;
        mpi_conn_arr[i].resps_suspended = 0;
        mpi_conn_arr[i].resp=0;
        mpi_conn_arr[i].resp_head=0;
        mpi_conn_arr[i].cmd = -1;
        mpi_conn_arr[i].rbuf_malloced = false;
        mpi_conn_arr[i].item_malloced = false;
        mpi_conn_arr[i].set_stale = false;
        mpi_conn_arr[i].mset_res = false;
        mpi_conn_arr[i].close_after_write = false;
        mpi_conn_arr[i].last_cmd_time = current_time;
        mpi_conn_arr[i].dpu_rank=1;
    }
}

static void mpi_conn_cleanup(struct mpi_conn_info *c) {
    assert(c != NULL);
    conn_release_items(c);
}


static void mpi_conn_close(struct mpi_conn_info *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_conn_close()\n");
#endif
    assert(c != NULL);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d connection closed.\n", c->rank);

    mpi_conn_cleanup(c);

    // force release of read buffer.
    if (c->thread) {
        c->rbytes = 0;
        mpi_rbuf_release(c);
    }

    c->state = conn_closed;

    c->close_reason = 0;

    pthread_mutex_lock(&conn_lock);
    allow_new_conns = true;
    pthread_mutex_unlock(&conn_lock);

    STATS_LOCK();
    stats_state.curr_conns--;
    STATS_UNLOCK();

    return;
}

static enum try_read_result mpi_try_read_network(struct mpi_conn_info* c){ 
#if ENABLE_PRINT
     printf("memcached/mpi_try_read_network, client->rbytes : %d | settings.oldest_live : %d\n",  c->rbytes, settings.oldest_live);
#endif
    enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
    int res;
    struct timespec probe_start, probe_end;
    struct timespec recv_start, recv_end;

    if (c->rcurr != c->rbuf) {
        if (c->rbytes > 0) /* otherwise there's nothing to copy */
            memmove(c->rbuf, c->rcurr, c->rbytes); //rcurr의 위치에서 rbytes 만큼의 바이트를 rbuf로 옮기기
        c->rcurr = c->rbuf;
    }

    while(1){
        int avail = c->rsize - c->rbytes;
        MPI_Status mpi_recv_status;
        memset(c->rbuf, 0, avail);
        unsigned long probe_elapsed = 0;
        unsigned long recv_elapsed = 0;
        #if BREAKDOWN
            if (memcached_measure_enabled) {
                clock_gettime(CLOCK_MONOTONIC, &probe_start);
            }
        #endif
        //printf("memcached/mpi_try_read_network, MPI_Probe\n");
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &mpi_recv_status);
        MPI_Get_count(&mpi_recv_status, MPI_CHAR, &res);
        #if BREAKDOWN
            if (memcached_measure_enabled) {
                clock_gettime(CLOCK_MONOTONIC, &probe_end);
                MPI_recv_time += memcached_elapsed_ns(&probe_start, &probe_end);
            }
        #endif

#if BREAKDOWN
        if (mpi_recv_status.MPI_TAG == DPU_MEASURE_START_TAG ||
                mpi_recv_status.MPI_TAG == DPU_MEASURE_END_TAG) {
            int err = MPI_Recv(c->rbuf, res, MPI_CHAR,
                    mpi_recv_status.MPI_SOURCE,
                    mpi_recv_status.MPI_TAG,
                    MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (err != MPI_SUCCESS) {
                char err_str[MPI_MAX_ERROR_STRING];
                int len;
                MPI_Error_string(err, err_str, &len);
                fprintf(stderr, "MPI_Recv error: %s\n", err_str);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }

            if (mpi_recv_status.MPI_TAG == DPU_MEASURE_START_TAG) {
                memcached_measure_start();
            } else {
                memcached_measure_end();
            }
            continue;
        }
#endif
        //printf("?\n");
        #if BREAKDOWN
            if (memcached_measure_enabled) {
                clock_gettime(CLOCK_MONOTONIC, &recv_start);
            }
        #endif
        int err = MPI_Recv(c->rbuf, res, MPI_CHAR,
                mpi_recv_status.MPI_SOURCE,
                mpi_recv_status.MPI_TAG,
                MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        //printf("[server] recv [%d] \"%s\" \n",mpi_recv_status.MPI_SOURCE, c->rbuf);
        #if BREAKDOWN
            if (memcached_measure_enabled && mpi_recv_status.MPI_TAG != END_TAG) {
                clock_gettime(CLOCK_MONOTONIC, &recv_end);
                MPI_recv_time += memcached_elapsed_ns(&recv_start, &recv_end);
            }
        #endif
        c->rank = mpi_recv_status.MPI_SOURCE; //메세지 요청을 보낸 랭크 값을 저장
        //printf("[server] DPU rank : %d\n",c->dpu_rank);
#if DPU_CACHE
        if(c->rank == 0){
            //printf("여기 들어가는지 확인 필요\n");
            c->rank = 2;
            c->dpu_rank=0;
        }else{
            c->dpu_rank = 1;
        }
#endif
        //memcpy(c->raw_rbuf, c->rbuf, res);

        if(mpi_recv_status.MPI_TAG == END_TAG){ //연산이 종료된 클라이언트는 마지막으로 END_TAG가 담긴 메세지를 전송
#if BREAKDOWN
            memcached_measure_end();
#endif
            completed_clients++;
            if(completed_clients == mpi_client_size){
#if ALL_TIME
                clock_gettime(CLOCK_REALTIME, &all_end);
                all_time+=((all_end.tv_sec-all_start.tv_sec)*BILLION)+all_end.tv_nsec-all_start.tv_nsec;
#endif
                return READ_ERROR;
            }
        }
        
        if (err != MPI_SUCCESS) {
            char err_str[MPI_MAX_ERROR_STRING];
            int len;
            MPI_Error_string(err, err_str, &len);
            fprintf(stderr, "MPI_Recv error: %s\n", err_str);
            fprintf(stderr, "MPI_SOURCE=%d, MPI_TAG=%d, MPI_ERROR=%d\n",
            mpi_recv_status.MPI_SOURCE,
            mpi_recv_status.MPI_TAG,
            mpi_recv_status.MPI_ERROR);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        //MPI_Get_count(&mpi_recv_status, MPI_CHAR, &res);
        //printf("res : %d\n", res);
        //int send_rank = mpi_recv_status.MPI_SOURCE;

        if(res > 0){
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.bytes_read += res;
            //c->raw_rbuf_size += res;
            pthread_mutex_unlock(&c->thread->stats.mutex);
            gotdata = READ_DATA_RECEIVED;
            c->rbytes += res;
            if (res == avail && c->rbuf_malloced) {
                // Resize rbuf and try a few times if huge ascii multiget.
                continue;
            } else {
                break;
            }
        }
        if (res == 0) {
            c->close_reason = NORMAL_CLOSE;
            return READ_ERROR;
        }
        if (res == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            return READ_ERROR;
        }

    }
    return gotdata;
}

// returns next response in chain.
mc_resp* mpi_resp_finish(struct mpi_conn_info *c, mc_resp *resp) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_resp_finish(), settings.oldest_live : %d\n", settings.oldest_live);
#endif
    mc_resp *next = resp->next;

    if (resp->item) { //여기 안들어가는듯?
        //printf("mpi_resp_finish2\n");
        // TODO: cache hash value in resp obj?
        item_remove(resp->item);
        resp->item = NULL;
    }

    if (resp->write_and_free) { //여기도 안들어가는듯
        free(resp->write_and_free);
    }

    if (resp->io_pending) { //여기도 안들어감
        io_pending_t *io = resp->io_pending;
        // If we had a pending IO, tell it to internally clean up then return
        // the main object back to our thread cache.
        io->finalize_cb(io);
        do_cache_free(c->thread->io_cache, io);
        resp->io_pending = NULL;
    }

    if (c->resp_head == resp) { //여기 들어감
        c->resp_head = next;
    }

    if (c->resp == resp) { //여기 들어감
        c->resp = NULL;
    }

    resp_free(c->thread, resp);
    return next;
}

static void mpi_transmit_post(struct mpi_conn_info *c, ssize_t res) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_transmit_post() \n");
#endif
    // We've written some of the data. Remove the completed
    // responses from the list of pending writes.
    mc_resp *resp = c->resp_head;

    while (resp) { //여기 들어감
        int x;
        //printf("mpi_transmit_post1\n");
        if (resp->skip) {
            //printf("mpi_transmit_post2\n");
            resp = mpi_resp_finish(c, resp);
            continue;
        }

        // fastpath check. all small responses should cut here.
        if (res >= resp->tosend) { //여기 들어감
            res -= resp->tosend;
            //printf("mpi_transmit_post3\n");
            resp = mpi_resp_finish(c, resp);
            continue;
        }

        // it's fine to re-check iov's that were zeroed out before.
        for (x = 0; x < resp->iovcnt; x++) {
            //printf("mpi_transmit_post4, resp->iovcnt : %d\n", resp->iovcnt);
            struct iovec *iov = &resp->iov[x];
            if (res >= iov->iov_len) {
                //printf("mpi_transmit_post4\n");
                resp->tosend -= iov->iov_len;
                res -= iov->iov_len;
                iov->iov_len = 0;
            } else {
                //printf("mpi_transmit_post5\n");
                // Dumb special case for chunked items. Currently tracking
                // where to inject the chunked item via iov_base.
                // Extra not-great since chunked items can't be the first
                // index, so we have to check for non-zero c_d_iov first.
                if (!resp->chunked_data_iov || x != resp->chunked_data_iov) {
                    iov->iov_base = (char *)iov->iov_base + res;
                }
                iov->iov_len -= res;
                resp->tosend -= res;
                res = 0;
                break;
            }
        }

        // are we done with this response object?
        if (resp->tosend == 0) {
            //printf("mpi_transmit_post6\n");
            resp = mpi_resp_finish(c, resp);
        } else {
            //printf("mpi_transmit_post7\n");
            // Jammed up here. This is the new head.
            break;
        }
    }
}


static int mpi_transmit_pre(struct mpi_conn_info *c, struct iovec *iovs, int iovused, int tid) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_transmit_pre()\n");
#endif
    mc_resp *resp = c->resp_head; //여기에 resp->wbuf가 그대로 담겨있는 것 같다
    //printf("mpi_trasnsmit_pre, resp->wbuf : %s\n", resp->wbuf); //여기서부터 resp->wbuf가 비어있는데 왜인지 모르겟슴
    while (resp && iovused + resp->iovcnt < IOV_MAX-1) {
       //printf("mpi_trasnsmit_pre1, iovused : %d | resp->iovcnt : %d | IOV_MAX-1 : %d\n", iovused, resp->iovcnt, IOV_MAX-1);
        if (resp->skip) { //안들어감
            //printf("mpi_trasnsmit_pre2\n");
            // Don't actually unchain the resp obj here since it's singly-linked.
            // Just let the post function handle it linearly.
            resp = resp->next;
            continue;
        }
        //printf("mpi_trasnsmit_pre debugging\n");
        if (resp->chunked_data_iov) { //안들어감
            //printf("mpi_trasnsmit_pre3\n");
            // Handle chunked items specially.
            // They spend much more time in send so we can be a bit wasteful
            // in rebuilding iovecs for them.
            //resp->chunked_data_iov 에서 짱 큰 값이 나옴 .. 왜죠
            item_chunk *ch = (item_chunk *)ITEM_schunk((item *)resp->iov[resp->chunked_data_iov].iov_base);
            int x;
            for (x = 0; x < resp->iovcnt; x++) {
                //printf("mpi_trasnsmit_pre4, resp->iovcnt : %d\n", resp->iovcnt);
                // This iov is tracking how far we've copied so far.
                if (x == resp->chunked_data_iov) {
                    //printf("mpi_trasnsmit_pre5\n");
                    int done = resp->chunked_total - resp->iov[x].iov_len;
                    // Start from the len to allow binprot to cut the \r\n
                    int todo = resp->iov[x].iov_len;
                    while (ch && todo > 0 && iovused < IOV_MAX-1) {
                        int skip = 0;
                        if (!ch->used) {
                            ch = ch->next;
                            continue;
                        }
                        // Skip parts we've already sent.
                        if (done >= ch->used) {
                            done -= ch->used;
                            ch = ch->next;
                            continue;
                        } else if (done) {
                            skip = done;
                            done = 0;
                        }
                        iovs[iovused].iov_base = ch->data + skip;
                        // Stupid binary protocol makes this go negative.
                        iovs[iovused].iov_len = ch->used - skip > todo ? todo : ch->used - skip;
                        iovused++;
                        todo -= ch->used - skip;
                        ch = ch->next;
                    }
                } else {
                    //printf("mpi_trasnsmit_pre6\n");
                    iovs[iovused].iov_base = resp->iov[x].iov_base;
                    iovs[iovused].iov_len = resp->iov[x].iov_len;
                    iovused++;
                }
                if (iovused >= IOV_MAX-1){
                    //printf("mpi_trasnsmit_pre7\n");
                    break;
                }
            }
        } else {
            // printf("mpi_trasnsmit_pre8\n");
            // for(int i=0; i<4; i++){
            //     printf("iov_base str = %s\n", (char *)resp->iov[i].iov_base);
            //     printf("iov_len = %zu\n", resp->iov[i].iov_len);
            // }
            //printf("mpi_trasnsmit_pre7 debugging 2\n");
            memcpy(&iovs[iovused], resp->iov, sizeof(struct iovec)*resp->iovcnt);
            iovused += resp->iovcnt;
        }
        //printf("mpi_trasnsmit_pre9\n");
        // done looking at first response, walk down the chain.
        resp = resp->next;
    }
    return iovused;
}


ssize_t mpi_sendmsg(struct mpi_conn_info *c, int tid) {
#if BREAKDOWN
    clock_gettime(CLOCK_REALTIME, &start);
#endif
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_sendmsg(), settings.oldest_live : %d\n", settings.oldest_live);
#endif
    /*여기는 실제 transmit 함수 부분. 내가 그냥 conn_mwrite/conn_write 에서 바로 메세지를 보낼 수 있게 통합함.*/
    //printf("mpi_sendmsg1\n");
    struct iovec iovs[IOV_MAX]; //IOV_MAX : 1024
    struct msghdr msg; //[yedam] 메세지 헤더(나중에 sendmsg에 필요함)
    int iovused = 0;
    int rank;
    struct timespec send_start, send_end;

    // init the msg.
    memset(&msg, 0, sizeof(struct msghdr));
    msg.msg_iov = iovs;
    //printf("mpi_sendmsg2\n");
    iovused = mpi_transmit_pre(c, iovs, iovused, tid);


    if (iovused == 0) { //여기 안들어감. iovused는 지금 처리해야하는 메세지 개수인데, 우리는 1개 들어오면 1개 처리하는 방식으로 일단 진행중이라서..
        //printf("mpi_sendmsg3\n");
        // printf("memcached/memcached.c :: transmit, _transmit_pre 이후 iovused : %d\n", iovused);
        // // Avoid the syscall if we're only handling a noreply.
        // // Return the response object.
        mpi_transmit_post(c, 0); //일단 이렇게 주석처리해두고, 나중에 다시 확인하기
        return TRANSMIT_COMPLETE;
    }

    char mpi_send_buf[1024];
    
    ssize_t offset=0;
    for (int i = 0; i < iovused; i++) {
        //printf("mpi_sendmsg4, iovused : %d\n", iovused);
        struct iovec *iov = &msg.msg_iov[i];
        memcpy(mpi_send_buf+offset, iov->iov_base, iov->iov_len);
        offset +=iov->iov_len;
    }

    assert (c != NULL);
    // bool measure_send = (get_req_num > 100000);
    // if (measure_send) {
    //     clock_gettime(CLOCK_REALTIME, &send_start);
    // }
    //printf("memcached server, [%d]에게 전송 메세지 : %s\n", c->rank, mpi_send_buf);
    // printf("[server] send [%d] \"%s\" \n", c-rank, mpi_send_buf);
    // printf("서버에서 저장중인 dpu의 현재 rank : %d\n", c->dpu_rank);
    #if BREAKDOWN
        if (memcached_measure_enabled) {
            clock_gettime(CLOCK_MONOTONIC, &send_start);
        }
    #endif
    MPI_Send(mpi_send_buf, offset, MPI_CHAR, c->rank, SEND_TAG, MPI_COMM_WORLD);
    #if BREAKDOWN
        if (memcached_measure_enabled) {
            clock_gettime(CLOCK_MONOTONIC, &send_end);
            MPI_send_time += memcached_elapsed_ns(&send_start, &send_end);
        }
    #endif
    get_req_num++;
    if(get_req_num > 100000){
        success++;
    }
    // if (measure_send) {
    //     clock_gettime(CLOCK_REALTIME, &send_end);
    //     send_brkdwn+=((send_end.tv_sec-send_start.tv_sec)*BILLION)+send_end.tv_nsec-send_start.tv_nsec;
    // }
#if DPU_CACHE
    if(c->dpu_rank==0){
        char dpu_send_buf[1024];
        ssize_t dpu_offset=0;
        memset(dpu_send_buf, 0, 1024);
        build_req_buffer(mpi_send_buf, dpu_send_buf, offset, &dpu_offset);
        //printf("memcached server, DPU[%d]에게 전송 메세지 : \"%s\" (%d) \n", c->dpu_rank, dpu_send_buf, dpu_offset);
        MPI_Send(dpu_send_buf, dpu_offset, MPI_CHAR, c->dpu_rank, SEND_TAG, MPI_COMM_WORLD);
    }
    c->dpu_rank=1;
#endif

    if (offset >= 0) { //여기 들어감

        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.bytes_written += offset;
        pthread_mutex_unlock(&c->thread->stats.mutex);
        //printf("mpi_sendmsg5\n");
        // Decrement any partial IOV's and complete any finished resp's.
        mpi_transmit_post(c, offset);

        if (c->resp_head) {
            //printf("mpi_sendmsg6-1\n");
            return TRANSMIT_INCOMPLETE;
        } else {
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                send_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
            //printf("mpi_sendmsg6-2\n");
            return TRANSMIT_COMPLETE;
        }
    }

    if (offset == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        c->state = conn_closing;
        return TRANSMIT_SOFT_ERROR;
    }
    /* if res == -1 and error is not EAGAIN or EWOULDBLOCK,
       we have a real error, on which we close the connection */
    if (settings.verbose > 0)
        perror("Failed to write, and not due to blocking");

    //conn_set_state(c, conn_closing);
    c->state = conn_closing;
    return TRANSMIT_HARD_ERROR;

}

static bool mpi_rbuf_alloc(struct mpi_conn_info *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_rbuf_alloc(), settings.oldest_live : %d\n", settings.oldest_live);
#endif
    if (c->rbuf == NULL) {
        c->rbuf = do_cache_alloc(c->thread->rbuf_cache);
        //c->raw_rbuf = malloc(READ_BUFFER_SIZE);
        if (!c->rbuf) {
            THR_STATS_LOCK(c->thread);
            c->thread->stats.read_buf_oom++;
            THR_STATS_UNLOCK(c->thread);
            return false;
        }
        c->rsize = READ_BUFFER_SIZE;
        c->rcurr = c->rbuf;
    }
    return true;
}

static void mpi_rbuf_release(struct mpi_conn_info *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_rbuf_release(), settings.oldest_live : %d\n", settings.oldest_live);
#endif
    pthread_t tid = pthread_self();

    if (c->rbuf != NULL && c->rbytes == 0) {
        if (c->rbuf_malloced) {
            free(c->rbuf);
            //free(c->raw_rbuf);
            c->rbuf_malloced = false;
        } else {
            do_cache_free(c->thread->rbuf_cache, c->rbuf); //여기서 오류가 생기는 것 같은데..
        }
        c->rsize = 0;
        c->rbuf = NULL;
        c->rcurr = NULL;
    }
}

static void mpi_reset_cmd_handler(struct mpi_conn_info *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_reset_cmd_handler(), settings.oldest_live : %d\n", settings.oldest_live);
#endif
    pthread_t tid = pthread_self();
    c->cmd = -1;
    c->substate = bin_no_state;
    if (c->item != NULL) {
        #if ENABLE_PRINT
            printf("memcached/memcached.c :: mpi_reset_cmd_handler()1\n");
        #endif
        if (c->item_malloced) {
            #if ENABLE_PRINT
                printf("memcached/memcached.c :: mpi_reset_cmd_handler()1-1\n");
            #endif
            free(c->item);
            c->item_malloced = false;
        } else {
            #if ENABLE_PRINT
                printf("memcached/memcached.c :: mpi_reset_cmd_handler()1-2\n");
            #endif
            item_remove(c->item);
        }
        c->item = NULL;
    }
    //상황에 따라 다르게 들어감, 주로 resp_head 아니면 else에 들어감
    if (c->rbytes > 0) {
        #if ENABLE_PRINT
            printf("memcached/memcached.c :: mpi_reset_cmd_handler()2\n");
        #endif
        c->state = conn_parse_cmd;
    } else if (c->resp_head) {
#if MPI_DEBUG
        printf("memcached/memcached.c :: mpi_reset_cmd_handler, c->resp_head->chunked_data_iov : %d\n", c->resp_head->chunked_data_iov);
#endif
        c->state = conn_mwrite;
    } else {
        #if ENABLE_PRINT
            printf("memcached/memcached.c :: mpi_reset_cmd_handler()3\n");
        #endif
        c->state = conn_waiting;
    }
}

static void mpi_drive_machine(struct mpi_conn_info *client, int tid){

    //struct mpi_conn_info *client = &mpi_conn_arr;
    enum try_read_result res;
    bool stop = false;
    const char *str;
    int nreqs = settings.reqs_per_event;
    pthread_t ttid = pthread_self();   // 현재 스레드 ID 얻기
    //printf("server debugging2\n");
    while(!stop){
        switch(client->state){
        case conn_listening:
            //printf("memcached::conn_listening()\n");
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            bool reject;
            if (settings.maxconns_fast) { //여기 들어가는게 맞음
                reject = client->rank >= settings.maxconns - 1;
                if (reject) {
                    STATS_LOCK();
                    stats.rejected_conns++;
                    STATS_UNLOCK();
                }
            } else { //여기 안들어감
                reject = false;
            }

            if (reject) { //여기 안들어감
                str = "ERROR Too many open connections\r\n";
                //res = write(client->rank, str, strlen(str));
                exit(0); //yedam
            } else {
                // run accept routine if ssl is compiled + enabled
                client->resp_head = 0; // 임시로 이렇게 설정
                mpi_dispatch_conn_new(client, conn_new_cmd, EV_READ | EV_PERSIST, READ_BUFFER_CACHED, client->transport, client->tag, client->protocol);
                client->state=conn_new_cmd;
            }
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
            break;
        case conn_read:
            //printf("memcached::conn_read()\n");
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            if (!mpi_rbuf_alloc(client)) { //여기는 안들어감
                client->state = conn_closing;
                break;
            }
            res = mpi_try_read_network(client);
            switch(res){
                case READ_NO_DATA_RECEIVED: //여기 안들어감
                    client->state = conn_waiting;
                    break;
                case READ_DATA_RECEIVED: //여기 들어감 -> conn_parse_cmd로 변경
                    client->state = conn_parse_cmd;
                    break;
                case READ_ERROR: //여기 안들어감
                    client->state = conn_closing;
                    break;
                case READ_MEMORY_ERROR: /* Failed to allocate more memory */ //여기 안들어감
                    /* State already set by try_read_network */
                    break;
            }
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                recv_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
            break;
            
        case conn_parse_cmd:
            //printf("memcached::conn_parse_cmd(), state : %d\n", client->state);
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            client->noreply = false;
            
            if (mpi_try_read_command_ascii(client, tid) == 0) {
                /* we need more data! */
                //printf("memcached::conn_parse_cmd()2, state : %d\n", client->state);
                if (client->resp_head) {
                    //printf("memcached::conn_parse_cmd()3, state : %d\n", client->state);
                    // Buffered responses waiting, flush in the meantime.
                    client->state = conn_mwrite;
                } else {
                    client->state = conn_waiting;
                }
            }
            //printf("memcached::conn_parse_cmd()4, state : %d\n", client->state);
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
            //printf("memcached::conn_parse_cmd(), state : %d\n", client->state);
            break;
            
        case conn_nread:
#if ENABLE_PRINT
            printf("memcached::conn_nread()\n");
#endif
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            if (client->rlbytes == 0) {
#if ENABLE_PRINT
            printf("memcached::conn_nread()2\n");
#endif
                mpi_complete_nread_ascii(client);
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }

            /* Check if rbytes < 0, to prevent crash */
            if (client->rlbytes < 0) {
#if ENABLE_PRINT
            printf("memcached::conn_nread()3\n");
#endif
                if (settings.verbose) {
                    fprintf(stderr, "Invalid rlbytes to read: len %d\n", client->rlbytes);
                }
                client->state = conn_closing;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }

            if (client->item_malloced || ((((item *)client->item)->it_flags & ITEM_CHUNKED) == 0) ) {
#if ENABLE_PRINT
            printf("memcached::conn_nread()4\n");
#endif
                /* first check if we have leftovers in the conn_read buffer */
                if (client->rbytes > 0) { //tocopy, rbytes, rlbytes 모두 동일한 크기를 가짐 
#if ENABLE_PRINT
            printf("memcached::conn_nread()5\n");
#endif
                    int tocopy = client->rbytes > client->rlbytes ? client->rlbytes : client->rbytes;
#if ENABLE_PRINT
            printf("memcached::conn_nread()5-1, tocopy : %d | client->rbytes : %d | client->rlbytes : %d\n", tocopy, client->rbytes, client->rlbytes);
#endif
                    memmove(client->ritem, client->rcurr, tocopy);
                    client->ritem += tocopy;
                    client->rlbytes -= tocopy;
                    client->rcurr += tocopy;
                    client->rbytes -= tocopy;
                    if (client->rlbytes == 0) {
                        #if BREAKDOWN
                            clock_gettime(CLOCK_REALTIME, &end);
                            other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                        #endif
                        break;
                    }
                }
                //printf("conn_nread :: mpu_try_read_network()\n");
                /*  now try reading from the socket */
#if ENABLE_PRINT
            printf("memcached::conn_nread()6\n");
#endif
                res = mpi_try_read_network(client);
                if (res > 0) {
#if ENABLE_PRINT
            printf("memcached::conn_nread()7\n");
#endif
                    pthread_mutex_lock(&client->thread->stats.mutex);
                    client->thread->stats.bytes_read += res;
                    pthread_mutex_unlock(&client->thread->stats.mutex);
                    if (client->rcurr == client->ritem) {
                        client->rcurr += res;
                    }
                    client->ritem += res;
                    client->rlbytes -= res;
                    #if BREAKDOWN
                        clock_gettime(CLOCK_REALTIME, &end);
                        other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                    #endif
                    break;
                }
            } else {
#if ENABLE_PRINT
            printf("memcached::conn_nread()8\n");
#endif
                res = mpi_read_into_chunked_item(client);
                if (res > 0){
                    #if BREAKDOWN
                        clock_gettime(CLOCK_REALTIME, &end);
                        other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                    #endif
                    break;
                }
            }

            if (res == 0) { /* end of stream */
                client->close_reason = NORMAL_CLOSE;
                client->state = conn_closing;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }

            if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                client->state = conn_closing;
                stop = true;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }

            /* Memory allocation failure */
            if (res == -2) {
                //out_of_memory(c, "SERVER_ERROR Out of memory during read");
                mpi_out_string(client, "SERVER_ERROR Out of memory during read");
                client->sbytes = client->rlbytes;
                client->state = conn_swallow;

                // Ensure this flag gets cleared. It gets killed on conn_new()
                // so any conn_closing is fine, calling complete_nread is
                // fine. This swallow semms to be the only other case.
                client->set_stale = false;
                client->mset_res = false;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (settings.verbose > 0) {
                fprintf(stderr, "Failed to read, and not due to blocking:\n"
                        "errno: %d %s \n"
                        "rcurr=%p ritem=%p rbuf=%p rlbytes=%d rsize=%d\n",
                        errno, strerror(errno),
                        (void *)client->rcurr, (void *)client->ritem, (void *)client->rbuf,
                        (int)client->rlbytes, (int)client->rsize);
            }
            client->state = conn_closing;
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
#if ENABLE_PRINT
            printf("memcached::conn_nread()9\n");
#endif
            break;
        case conn_swallow:
            //printf("memcached::swallow()\n");
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            /* we are reading sbytes and throwing them away */
            if (client->sbytes <= 0) {
                client->state = conn_new_cmd;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }

            /* first check if we have leftovers in the conn_read buffer */
            if (client->rbytes > 0) {
                int tocopy = client->rbytes > client->sbytes ? client->sbytes : client->rbytes;
                client->sbytes -= tocopy;
                client->rcurr += tocopy;
                client->rbytes -= tocopy;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }
            /*  now try reading from the socket */
            //res = c->read(c, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize);
            res = mpi_try_read_network(client);
            if (res > 0) {
                pthread_mutex_lock(&client->thread->stats.mutex);
                client->thread->stats.bytes_read += res;
                pthread_mutex_unlock(&client->thread->stats.mutex);
                client->sbytes -= res;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }
            if (res == 0) { /* end of stream */
                client->close_reason = NORMAL_CLOSE;
                client->state = conn_closing;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }
            if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                client->state = conn_closing;
                stop = true;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (settings.verbose > 0)
                fprintf(stderr, "Failed to read, and not due to blocking\n");

            client->state = conn_closing;

            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
            break;
        case conn_new_cmd:
            //printf("memcached::conn_new_cmd()\n");
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            //printf("memcached/conn_new_cmd, nreqs : %d\n", nreqs);
            --nreqs;
            if (nreqs >= 0) {
                //printf("1\n");
                mpi_reset_cmd_handler(client); //여기로 들어감
            } else if (client->resp_head) { //여기 안들어감
                //printf("2\n");
                // flush response pipe on yield.
                client->state = conn_mwrite;
            } else { //여기 안들어감
                //printf("3\n");
                pthread_mutex_lock(&client->thread->stats.mutex);
                client->thread->stats.conn_yields++;
                pthread_mutex_unlock(&client->thread->stats.mutex);
                if (client->rbytes > 0) {
                    client->state = conn_closing;
                }
                stop = true;
            }
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
            break;

        case conn_waiting:
            //printf("memcached::conn_waiting()\n");
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            mpi_rbuf_release(client);
            client->state=conn_read;
            stop = true;
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
            break;
        case conn_write:
        case conn_mwrite:
            //printf("memcached::conn_mwrite()\n");
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            if (client->resps_suspended) {
                LIBEVENT_THREAD *t = client->thread;
                // FIXME: carefully check or document somewhere that submit
                // cannot resume a connection in-line with submission.
                client->state = conn_io_queue;
                //conn_set_state(c, conn_io_queue);
                if (t->conns_tosubmit++ > 20) {
                    // Run occasional batches, else submit outside event loop.
                    thread_io_queue_submit(client->thread);
                }

                stop = true;
                #if BREAKDOWN
                    clock_gettime(CLOCK_REALTIME, &end);
                    send_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
                #endif
                break;
            }

            switch (mpi_sendmsg(client, tid)) { //요기 들어감
            case TRANSMIT_COMPLETE:
                if (client->state == conn_mwrite) { //요기 들어감
                    //if(client->item)
                    // Free up IO wraps and any half-uploaded items.
                    mpi_conn_release_items(client);
                    client->state = conn_new_cmd;
                    if (client->close_after_write) {
                        client->state = conn_closing;
                    }
                } else {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Unexpected state %d\n", client->state);
                    client->state = conn_closing;

                }
                //printf("memcached/conn_mwrite, nreqs : %d\n", nreqs);
                nreqs++; //이걸 주석처리하지 않으면 settings.oldest_live 값이 계속 올라가는 문제가 발생함. 일단 주석처리했는데 이유를 더 상세하게 알아야할듯
                break;

            case TRANSMIT_INCOMPLETE:
            case TRANSMIT_HARD_ERROR:
                break;                   /* Continue in state machine. */

            case TRANSMIT_SOFT_ERROR:
                stop = true;
                break;
            }
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                send_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif
            break;
        case conn_closing:
            //printf("memcached::conn_closing()\n");
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &start);
            #endif
            //client->state=conn_listening; //이거 잠시 임시로 해둠
            //conn_close(client);
            stop = true;
            #if BREAKDOWN
                clock_gettime(CLOCK_REALTIME, &end);
                other_brkdwn+=((end.tv_sec-start.tv_sec)*BILLION)+end.tv_nsec-start.tv_nsec;
            #endif

            #if HASH_BREAKDOWN
                printf("assoc_find : %lu sec\n", hash_time[temp_rank]);
            #endif
            
            #if ALL_TIME
            printf("server elapsed_time : %lu sec\n", all_time);
            #endif

            //sleep(10);
            exit(0);
            break;

        case conn_closed:
            /* This only happens if dormando is an idiot. */
            abort();
            break;
        case conn_io_queue:
            /* Woke up while waiting for an async return, but not ready. */
            client->state = conn_io_pending;
            stop = true;
            break;
        case conn_io_pending:
            /* Should not be reachable */
            assert(false);
            break;
        }
    
    }
}

void mpi_out_string(struct mpi_conn_info *c, const char *str) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_out_string(), settings.oldest_live : %d\n", settings.oldest_live);
#endif
    size_t len;
    assert(c != NULL);
    mc_resp *resp = c->resp;
    // if response was original filled with something, but we're now writing
    // out an error or similar, have to reset the object first.
    // TODO: since this is often redundant with allocation, how many callers
    // are actually requiring it be reset? Can we fast test by just looking at
    // tosend and reset if nonzero?
    resp_reset(resp);
    if (c->noreply) {
        // TODO: just invalidate the response since nothing's been attempted
        // to send yet?
        resp->skip = true;
        if (settings.verbose > 1)
            fprintf(stderr, ">%d NOREPLY %s\n", c->rank, str);
        //conn_set_state(c, conn_new_cmd);
        c->state = conn_new_cmd;
        return;
    }

    if (settings.verbose > 1)
    // Fill response object with static string.

    len = strlen(str);
    if ((len + 2) > WRITE_BUFFER_SIZE) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = strlen(str);
    }
    memcpy(resp->wbuf, str, strlen(str));
    memcpy(resp->wbuf + strlen(str), "\r\n", 2);
    //printf("out_string(), resp->wbuf : %s\n", resp->wbuf);
    resp_add_iov(resp, resp->wbuf, strlen(str) + 2, 6);
    c->state = conn_new_cmd;
    return;
}

// 현재 스레드가 관리하는 responce 객체 풀에서, 비어있는 mc_resp 하나를 꺼내어 초기화하는 함수
// mc_resp : resp 객체 하나
// mc_resp_bundle : resp 객체들을 묶어놓은 객체
// mc_resp_bundle에 있는 r[]이 실제 responce 객체 배열임. 그리고 거기 있는 free 여부가 지금 비어있는지를 뜻하는거
// refcount는 현재 이 bundle 안에서 사용중인 mc_resp 개수를 뜻함.
// open_bundle은 스레드가 가지고 있는 번들 중, 빈 mc_resp가 남아있어서 할당 가능한 bundle의 머리를 뜻함
static mc_resp* mpi_resp_allocate(struct mpi_conn_info *c) {
    LIBEVENT_THREAD *th = c->thread;
    mc_resp *resp = NULL;
    mc_resp_bundle *b = th->open_bundle;

    if (b != NULL) { //여기 들어감
        for (int i = 0; i < MAX_RESP_PER_BUNDLE; i++) { //bundle 안의 r[]을 순회
            // loop around starting from the most likely to be free
            int x = (i + b->next_check) % MAX_RESP_PER_BUNDLE; //이건 이전에 마지막으로 찾은 위치 다음부터 순회하겠다는 의미
            if (b->r[x].free) { //free인 공간이 남아있는 경우 그 칸 주소를 resp에 저장하고, bundle의 다음 resp에 x+1을 연결
                resp = &b->r[x];
                b->next_check = x+1;
                break;
            }
        }

        if (resp != NULL) { //여기 들어감
            b->refcount++;
            memset(resp, 0, sizeof(*resp)); //초기화 진행하기
            resp->free = false; // redundant, for clarity.
            resp->bundle = b;
            if (b->refcount == MAX_RESP_PER_BUNDLE) { //근데 여긴 안들어감
                assert(b->prev == NULL);
                // We only allocate off the head. Assign new head.
                th->open_bundle = b->next;
                // Remove ourselves from the list.
                if (b->next) {
                    b->next->prev = 0;
                    b->next = 0;
                }
            }
        }
    }
    if (resp == NULL) { //여기 안들어감
        assert(th->open_bundle == NULL);
        b = do_cache_alloc(th->rbuf_cache);
        if (b) {
            THR_STATS_LOCK(th);
            th->stats.response_obj_bytes += READ_BUFFER_SIZE;
            THR_STATS_UNLOCK(th);
            b->next_check = 1;
            b->refcount = 1;
            for (int i = 0; i < MAX_RESP_PER_BUNDLE; i++) {
                b->r[i].free = true;
            }
            b->next = 0;
            b->prev = 0;
            b->thread = th;
            th->open_bundle = b;
            resp = &b->r[0];
            memset(resp, 0, sizeof(*resp));
            resp->free = false; // redundant. for clarity.
            resp->bundle = b;
            resp->tosend=0;
        } else {
            return NULL;
        }
    }
    //여기로 빠져나옴
    resp->tosend=0;
    return resp;    
}

bool mpi_resp_start(struct mpi_conn_info *c) {
#if ENABLE_PRINT
    printf("memcached/memcached.c :: mpi_resp_start(), settings.oldest_live : %d\n", settings.oldest_live);
#endif
    mc_resp *resp = mpi_resp_allocate(c);
    if (!resp) {
        THR_STATS_LOCK(c->thread);
        c->thread->stats.response_obj_oom++;
        THR_STATS_UNLOCK(c->thread);
        return false;
    }
    // handling the stats counters here to simplify testing
    THR_STATS_LOCK(c->thread);
    c->thread->stats.response_obj_count++;
    THR_STATS_UNLOCK(c->thread);
    if (!c->resp_head) {
        c->resp_head = resp;
    }
    if (!c->resp) {
        c->resp = resp;
    } else {
        c->resp->next = resp;
        c->resp = resp;
    }

    c->resp->tosend=0; //error 발생으로 잠시 둠
    return true;
}

static volatile bool running = true;
static time_t monotonic_start;
void* assoc_expand_check_thread(void *arg){
    struct timespec ts;
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};

    while (running) {
        sleep(1);

        struct timeval tv;
        gettimeofday(&tv, NULL);

        current_time = (rel_time_t) (tv.tv_sec - process_started);
        //printf("current_time : %d\n", current_time);

        // hash table expansion check
        assoc_start_expand(stats_state.curr_items);

        if (settings.sig_hup) {
            settings.sig_hup = false;
            authfile_load(settings.auth_file);
        }

    }
    return NULL;
}

pthread_t expand_tid;
static void mpi_clock_handler(const evutil_socket_t fd, const short which, void *arg) {

    //printf("memcached/memcached.c :: mpi_clock_handler()\n");
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};

    pthread_create(&expand_tid, NULL, assoc_expand_check_thread, NULL);

    // While we're here, check for hash table expansion.
    // This function should be quick to avoid delaying the timer.
    //assoc_start_expand(stats_state.curr_items);
    // also, if HUP'ed we need to do some maintenance.
    // for now that's just the authfile reload.
    if (settings.sig_hup) {
        settings.sig_hup = false;
        authfile_load(settings.auth_file);
    }

#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
    if (monotonic) {
        struct timespec ts;
        if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1)
            return;
#ifdef MEMCACHED_DEBUG
        current_time = (rel_time_t) (ts.tv_sec - monotonic_start + delta);
#else
        current_time = (rel_time_t) (ts.tv_sec - monotonic_start);
#endif
        return;
    }
#endif
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
#ifdef MEMCACHED_DEBUG
        current_time = (rel_time_t) (tv.tv_sec - process_started + delta);
#else
        current_time = (rel_time_t) (tv.tv_sec - process_started);
#endif
    }

}





#endif
























int main (int argc, char **argv) {

#if ALL_TIME
    clock_gettime(CLOCK_REALTIME, &all_start);
#endif
    int c;
    bool lock_memory = false;
    bool do_daemonize = false;
    bool preallocate = false;
    int maxcore = 0;
    char *username = NULL;
    char *pid_file = NULL;
    struct passwd *pw;
    struct rlimit rlim;
    char *buf;
    char unit = '\0';
    int size_max = 0;
    int retval = EXIT_SUCCESS;
    bool protocol_specified = false;
    bool tcp_specified = false;
    bool udp_specified = false;
    bool start_lru_maintainer = true;
    bool start_lru_crawler = true;
    bool start_assoc_maint = true;
    enum hashfunc_type hash_type = MURMUR3_HASH;
    uint32_t tocrawl;
    uint32_t slab_sizes[MAX_NUMBER_OF_SLAB_CLASSES];
    bool use_slab_sizes = false;
    char *slab_sizes_unparsed = NULL;
    bool slab_chunk_size_changed = false;
    printf("나 서버\n");
    // struct for restart code. Initialized up here so we can curry
    // important settings to save or validate.
    struct _mc_meta_data *meta = malloc(sizeof(struct _mc_meta_data));
    meta->slab_config = NULL;
    char *subopts, *subopts_orig;
    char *subopts_value;
#if ENABLE_MPI_FUNCTIONS
    //yedam
        int rank;
        int size;
        //char processor_name[MPI_MAX_PROCESSOR_NAME];
        //int name_len;

        MPI_Init(&argc, &argv); // 초기화
        //int mpi_provided;
        //MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_provided);

        // if (mpi_provided < MPI_THREAD_MULTIPLE) {
        //     fprintf(stderr, "Error: MPI does not support THREAD_MULTIPLE\n");
        //     MPI_Abort(MPI_COMM_WORLD, 1);
        // }
        MPI_Comm_rank(MPI_COMM_WORLD, &rank); // 내 rank를 MPI_COMM_WORLD라는 통신 그룹에 넣기
        MPI_Comm_size(MPI_COMM_WORLD, &size); // 현재 MPI_COMM_WORLD라는 통신 그룹에 몇개의 rank가 있는
        int memcached_server_size;
#if HASH_BREAKDOWN
        temp_rank = rank;
#endif
#if MEMCACHED_MODE
    memcached_server_size = 1;
#endif

#if PS_MODE
    memcached_server_size = size-2;
#endif
        mpi_conn_arr = (struct mpi_conn_info*)malloc(sizeof(struct mpi_conn_info)*memcached_server_size);

        mpi_conn_init(mpi_conn_arr, size);

        

        // MPI_Comm new_comm;
        // MPI_Comm_split(MPI_COMM_WORLD, mpi_color, rank, &new_comm);

        // if (mpi_color != MPI_UNDEFINED) {
        //     MPI_Barrier(new_comm);  // 새 communicator에 포함된 랭크들만 barrier
        // }else{
        //     printf("Rank %d skipped the barrier\n", rank);
        // }

        //MPI_Get_processor_name(processor_name, &name_len);

        #if ENABLE_PRINT
            printf("memcached/memcached.c :: main()-[2], mpi_conn_arr[0]->item : %d\n", mpi_conn_arr[0].item);
        #endif

    #endif

    enum {
        MAXCONNS_FAST = 0,
        HASHPOWER_INIT,
        NO_HASHEXPAND,
        SLAB_REASSIGN,
        SLAB_AUTOMOVE,
        SLAB_AUTOMOVE_RATIO,
        SLAB_AUTOMOVE_WINDOW,
        TAIL_REPAIR_TIME,
        HASH_ALGORITHM,
        LRU_CRAWLER,
        LRU_CRAWLER_SLEEP,
        LRU_CRAWLER_TOCRAWL,
        LRU_MAINTAINER,
        HOT_LRU_PCT,
        WARM_LRU_PCT,
        HOT_MAX_FACTOR,
        WARM_MAX_FACTOR,
        TEMPORARY_TTL,
        IDLE_TIMEOUT,
        WATCHER_LOGBUF_SIZE,
        WORKER_LOGBUF_SIZE,
        SLAB_SIZES,
        SLAB_CHUNK_MAX,
        TRACK_SIZES,
        NO_INLINE_ASCII_RESP,
        MODERN,
        NO_MODERN,
        NO_CHUNKED_ITEMS,
        NO_SLAB_REASSIGN,
        NO_SLAB_AUTOMOVE,
        NO_MAXCONNS_FAST,
        INLINE_ASCII_RESP,
        NO_LRU_CRAWLER,
        NO_LRU_MAINTAINER,
        NO_DROP_PRIVILEGES,
        DROP_PRIVILEGES,
        RESP_OBJ_MEM_LIMIT,
        READ_BUF_MEM_LIMIT,
#ifdef TLS
        SSL_CERT,
        SSL_KEY,
        SSL_VERIFY_MODE,
        SSL_KEYFORM,
        SSL_CIPHERS,
        SSL_CA_CERT,
        SSL_WBUF_SIZE,
        SSL_SESSION_CACHE,
        SSL_KERNEL_TLS,
        SSL_MIN_VERSION,
#endif
#ifdef PROXY
        PROXY_CONFIG,
        PROXY_ARG,
        PROXY_URING,
        PROXY_MEMPROFILE,
#endif
#ifdef MEMCACHED_DEBUG
        RELAXED_PRIVILEGES,
#endif
#ifdef SOCK_COOKIE_ID
        COOKIE_ID,
#endif
    };
    char *const subopts_tokens[] = {
        [MAXCONNS_FAST] = "maxconns_fast",
        [HASHPOWER_INIT] = "hashpower",
        [NO_HASHEXPAND] = "no_hashexpand",
        [SLAB_REASSIGN] = "slab_reassign",
        [SLAB_AUTOMOVE] = "slab_automove",
        [SLAB_AUTOMOVE_RATIO] = "slab_automove_ratio",
        [SLAB_AUTOMOVE_WINDOW] = "slab_automove_window",
        [TAIL_REPAIR_TIME] = "tail_repair_time",
        [HASH_ALGORITHM] = "hash_algorithm",
        [LRU_CRAWLER] = "lru_crawler",
        [LRU_CRAWLER_SLEEP] = "lru_crawler_sleep",
        [LRU_CRAWLER_TOCRAWL] = "lru_crawler_tocrawl",
        [LRU_MAINTAINER] = "lru_maintainer",
        [HOT_LRU_PCT] = "hot_lru_pct",
        [WARM_LRU_PCT] = "warm_lru_pct",
        [HOT_MAX_FACTOR] = "hot_max_factor",
        [WARM_MAX_FACTOR] = "warm_max_factor",
        [TEMPORARY_TTL] = "temporary_ttl",
        [IDLE_TIMEOUT] = "idle_timeout",
        [WATCHER_LOGBUF_SIZE] = "watcher_logbuf_size",
        [WORKER_LOGBUF_SIZE] = "worker_logbuf_size",
        [SLAB_SIZES] = "slab_sizes",
        [SLAB_CHUNK_MAX] = "slab_chunk_max",
        [TRACK_SIZES] = "track_sizes",
        [NO_INLINE_ASCII_RESP] = "no_inline_ascii_resp",
        [MODERN] = "modern",
        [NO_MODERN] = "no_modern",
        [NO_CHUNKED_ITEMS] = "no_chunked_items",
        [NO_SLAB_REASSIGN] = "no_slab_reassign",
        [NO_SLAB_AUTOMOVE] = "no_slab_automove",
        [NO_MAXCONNS_FAST] = "no_maxconns_fast",
        [INLINE_ASCII_RESP] = "inline_ascii_resp",
        [NO_LRU_CRAWLER] = "no_lru_crawler",
        [NO_LRU_MAINTAINER] = "no_lru_maintainer",
        [NO_DROP_PRIVILEGES] = "no_drop_privileges",
        [DROP_PRIVILEGES] = "drop_privileges",
        [RESP_OBJ_MEM_LIMIT] = "resp_obj_mem_limit",
        [READ_BUF_MEM_LIMIT] = "read_buf_mem_limit",
#ifdef TLS
        [SSL_CERT] = "ssl_chain_cert",
        [SSL_KEY] = "ssl_key",
        [SSL_VERIFY_MODE] = "ssl_verify_mode",
        [SSL_KEYFORM] = "ssl_keyformat",
        [SSL_CIPHERS] = "ssl_ciphers",
        [SSL_CA_CERT] = "ssl_ca_cert",
        [SSL_WBUF_SIZE] = "ssl_wbuf_size",
        [SSL_SESSION_CACHE] = "ssl_session_cache",
        [SSL_KERNEL_TLS] = "ssl_kernel_tls",
        [SSL_MIN_VERSION] = "ssl_min_version",
#endif
#ifdef PROXY
        [PROXY_CONFIG] = "proxy_config",
        [PROXY_ARG] = "proxy_arg",
        [PROXY_URING] = "proxy_uring",
        [PROXY_MEMPROFILE] = "proxy_memprofile",
#endif
#ifdef MEMCACHED_DEBUG
        [RELAXED_PRIVILEGES] = "relaxed_privileges",
#endif
#ifdef SOCK_COOKIE_ID
        [COOKIE_ID] = "sock_cookie_id",
#endif
        NULL
    };

    if (!sanitycheck()) { //안 들어감
        free(meta);
        return EX_OSERR;
    }

    /* handle SIGINT, SIGTERM */
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGHUP, sighup_handler);
    signal(SIGUSR1, sig_usrhandler);

    /* init settings */
    settings_init(); //port 번호는 11211로, 최대 연결 개수는 1024개로 기본 워커 스레드는 4개 등등 기본적인 설정 초기화 진행하는 함수
    //settings.num_threads = size-1; //스레드 개수만큼 worker 생성을 일단 했음..
    verify_default("hash_algorithm", hash_type == MURMUR3_HASH); //해시 알고리즘은 멀멀3 으로 진행
    void *storage = NULL;
#ifdef EXTSTORE

    void *storage_cf = storage_init_config(&settings); //들어감
    bool storage_enabled = false;
    if (storage_cf == NULL) {
        fprintf(stderr, "failed to allocate extstore config\n");
        return 1;
    }
#endif

    /* set stderr non-buffering (for running under, say, daemontools) */
    setbuf(stderr, NULL);

    char *shortopts =
          "a:"  /* access mask for unix socket */
          "A"   /* enable admin shutdown command */
          "Z"   /* enable SSL */
          "p:"  /* TCP port number to listen on */
          "s:"  /* unix socket path to listen on */
          "U:"  /* UDP port number to listen on */
          "m:"  /* max memory to use for items in megabytes */
          "M"   /* return error on memory exhausted */
          "c:"  /* max simultaneous connections */
          "k"   /* lock down all paged memory */
          "hiV" /* help, licence info, version */
          "r"   /* maximize core file limit */
          "v"   /* verbose */
          "d"   /* daemon mode */
          "l:"  /* interface to listen on */
          "u:"  /* user identity to run as */
          "P:"  /* save PID in file */
          "f:"  /* factor? */
          "n:"  /* minimum space allocated for key+value+flags */
          "t:"  /* threads */
          "D:"  /* prefix delimiter? */
          "L"   /* Large memory pages */
          "R:"  /* max requests per event */
          "C"   /* Disable use of CAS */
          "b:"  /* backlog queue limit */
          "B:"  /* Binding protocol */
          "I:"  /* Max item size */
          "S"   /* Sasl ON */
          "F"   /* Disable flush_all */
          "X"   /* Disable dump commands */
          "W"   /* Disable watch commands */
          "Y:"   /* Enable token auth */
          "e:"  /* mmap path for external item memory */
          "o:"  /* Extended generic options */
          "N:"  /* NAPI ID based thread selection */
          ;

    /* process arguments */
#ifdef HAVE_GETOPT_LONG //여기로 들어감
    const struct option longopts[] = {
        {"unix-mask", required_argument, 0, 'a'},
        {"enable-shutdown", no_argument, 0, 'A'},
        {"enable-ssl", no_argument, 0, 'Z'},
        {"port", required_argument, 0, 'p'},
        {"unix-socket", required_argument, 0, 's'},
        {"udp-port", required_argument, 0, 'U'},
        {"memory-limit", required_argument, 0, 'm'},
        {"disable-evictions", no_argument, 0, 'M'},
        {"conn-limit", required_argument, 0, 'c'},
        {"lock-memory", no_argument, 0, 'k'},
        {"help", no_argument, 0, 'h'},
        {"license", no_argument, 0, 'i'},
        {"version", no_argument, 0, 'V'},
        {"enable-coredumps", no_argument, 0, 'r'},
        {"verbose", optional_argument, 0, 'v'},
        {"daemon", no_argument, 0, 'd'},
        {"listen", required_argument, 0, 'l'},
        {"user", required_argument, 0, 'u'},
        {"pidfile", required_argument, 0, 'P'},
        {"slab-growth-factor", required_argument, 0, 'f'},
        {"slab-min-size", required_argument, 0, 'n'},
        {"threads", required_argument, 0, 't'},
        {"enable-largepages", no_argument, 0, 'L'},
        {"max-reqs-per-event", required_argument, 0, 'R'},
        {"disable-cas", no_argument, 0, 'C'},
        {"listen-backlog", required_argument, 0, 'b'},
        {"protocol", required_argument, 0, 'B'},
        {"max-item-size", required_argument, 0, 'I'},
        {"enable-sasl", no_argument, 0, 'S'},
        {"disable-flush-all", no_argument, 0, 'F'},
        {"disable-dumping", no_argument, 0, 'X'},
        {"disable-watch", no_argument, 0, 'W'},
        {"auth-file", required_argument, 0, 'Y'},
        {"memory-file", required_argument, 0, 'e'},
        {"extended", required_argument, 0, 'o'},
        {"napi-ids", required_argument, 0, 'N'},
        {0, 0, 0, 0}
    };
    int optindex;
    while (-1 != (c = getopt_long(argc, argv, shortopts, longopts, &optindex))) {

#else
    while (-1 != (c = getopt(argc, argv, shortopts))) {
#endif
        if (optarg) {

            while(isspace(optarg[0])) {
                optarg++;
            }
        }
        switch (c) {
        //yedam
        // case 'np':
        //     int ranks = atoi(optarg);
        //     printf("memcached/memcached.c :: int main - np parameter is %d\n", ranks);
        case 'A':
            /* enables "shutdown" command */
            settings.shutdown_command = true;
            break;
        case 'Z':
            /* enable secure communication*/
#ifdef TLS
            settings.ssl_enabled = true;
#else
            fprintf(stderr, "This server is not built with TLS support.\n");
            exit(EX_USAGE);
#endif
            break;
        case 'a':
#ifndef DISABLE_UNIX_SOCKET
            /* access for unix domain socket, as octal mask (like chmod)*/
            settings.access= strtol(optarg,NULL,8);
#else
            fprintf(stderr, "This server is not built with unix socket support.\n");
            exit(EX_USAGE);
#endif /* #ifndef DISABLE_UNIX_SOCKET */
            break;
        case 'U':
            settings.udpport = atoi(optarg);
            udp_specified = true;
            break;
        case 'p':
            settings.port = atoi(optarg);
            tcp_specified = true;
            break;
        case 's':
#ifndef DISABLE_UNIX_SOCKET
            settings.socketpath = optarg;
#else
            fprintf(stderr, "This server is not built with unix socket support.\n");
            exit(EX_USAGE);
#endif /* #ifndef DISABLE_UNIX_SOCKET */
            break;
        case 'm':
            settings.maxbytes = ((size_t)atoi(optarg)) * 1024 * 1024;
            break;
        case 'M':
            settings.evict_to_free = 0;
            break;
        case 'c':
            settings.maxconns = atoi(optarg);
            if (settings.maxconns <= 0) {
                fprintf(stderr, "Maximum connections must be greater than 0\n");
                return 1;
            }
            break;
        case 'h':
            usage();
            exit(EXIT_SUCCESS);
        case 'i':
            usage_license();
            exit(EXIT_SUCCESS);
        case 'V':
            printf(PACKAGE " " VERSION "\n");
            exit(EXIT_SUCCESS);
        case 'k':
            lock_memory = true;
            break;
        case 'v':
            //settings.verbose++;
            settings.verbose=1;
            break;
        case 'l':
            if (settings.inter != NULL) { //여기 안들어감
                if (strstr(settings.inter, optarg) != NULL) {
                    break;
                }
                size_t len = strlen(settings.inter) + strlen(optarg) + 2;
                char *p = malloc(len);
                if (p == NULL) {
                    fprintf(stderr, "Failed to allocate memory\n");
                    return 1;
                }
                snprintf(p, len, "%s,%s", settings.inter, optarg);
                free(settings.inter);
                settings.inter = p;
            } else { //여기 들어감
                settings.inter= strdup(optarg); //여기에 있는 settings.inter가 ip주소가 됨
            }
            break;
        case 'd':
            do_daemonize = true;
            break;
        case 'r':
            maxcore = 1;
            break;
        case 'R':
            settings.reqs_per_event = atoi(optarg);
            if (settings.reqs_per_event <= 0) {
                fprintf(stderr, "Number of requests per event must be greater than 0\n");
                return 1;
            }
            break;
        case 'u':
            username = optarg;
            break;
        case 'P':
            pid_file = optarg;
            break;
        case 'e':
            settings.memory_file = optarg;
            break;
        case 'f':
            settings.factor = atof(optarg);
            if (settings.factor <= 1.0) {
                fprintf(stderr, "Factor must be greater than 1\n");
                return 1;
            }
            meta->slab_config = strdup(optarg);
            break;
        case 'n':
            settings.chunk_size = atoi(optarg);
            if (settings.chunk_size <= 0) {
                fprintf(stderr, "Chunk size must be greater than 0\n");
                return 1;
            }
            break;
        case 't':
            settings.num_threads = atoi(optarg);
            if (settings.num_threads <= 0) {
                fprintf(stderr, "Number of threads must be greater than 0\n");
                return 1;
            }
            /* There're other problems when you get above 64 threads.
             * In the future we should portably detect # of cores for the
             * default.
             */
            if (settings.num_threads > 64) {
                fprintf(stderr, "WARNING: Setting a high number of worker"
                                "threads is not recommended.\n"
                                " Set this value to the number of cores in"
                                " your machine or less.\n");
            }
            break;
        case 'D':
            if (! optarg || ! optarg[0]) {
                fprintf(stderr, "No delimiter specified\n");
                return 1;
            }
            settings.prefix_delimiter = optarg[0];
            settings.detail_enabled = 1;
            break;
        case 'L' :
            if (enable_large_pages() == 0) {
                preallocate = true;
            } else {
                fprintf(stderr, "Cannot enable large pages on this system\n"
                    "(There is no support as of this version)\n");
                return 1;
            }
            break;
        case 'C' :
            settings.use_cas = false;
            break;
        case 'b' :
            settings.backlog = atoi(optarg);
            break;
        case 'B':
            protocol_specified = true;
            if (strcmp(optarg, "auto") == 0) {
                settings.binding_protocol = negotiating_prot;
            } else if (strcmp(optarg, "binary") == 0) {
                settings.binding_protocol = binary_prot;
            } else if (strcmp(optarg, "ascii") == 0) {
                settings.binding_protocol = ascii_prot;
            } else {
                fprintf(stderr, "Invalid value for binding protocol: %s\n"
                        " -- should be one of auto, binary, or ascii\n", optarg);
                exit(EX_USAGE);
            }
            break;
        case 'I':
            buf = strdup(optarg);
            unit = buf[strlen(buf)-1];
            if (unit == 'k' || unit == 'm' ||
                unit == 'K' || unit == 'M') {
                buf[strlen(buf)-1] = '\0';
                size_max = atoi(buf);
                if (unit == 'k' || unit == 'K')
                    size_max *= 1024;
                if (unit == 'm' || unit == 'M')
                    size_max *= 1024 * 1024;
                settings.item_size_max = size_max;
            } else {
                settings.item_size_max = atoi(buf);
            }
            free(buf);
            break;
        case 'S': /* set Sasl authentication to true. Default is false */
#ifndef ENABLE_SASL
            fprintf(stderr, "This server is not built with SASL support.\n");
            exit(EX_USAGE);
#endif
            settings.sasl = true;
            break;
       case 'F' :
            settings.flush_enabled = false;
            break;
       case 'X' :
            settings.dump_enabled = false;
            break;
       case 'W' :
            settings.watch_enabled = false;
            break;
       case 'Y' :
            // dupe the file path now just in case the options get mangled.
            settings.auth_file = strdup(optarg);
            break;
       case 'N':
            settings.num_napi_ids = atoi(optarg);
            if (settings.num_napi_ids <= 0) {
                fprintf(stderr, "Maximum number of NAPI IDs must be greater than 0\n");
                return 1;
            }
            break;
        case 'o': /* It's sub-opts time! */
            subopts_orig = subopts = strdup(optarg); /* getsubopt() changes the original args */

            while (*subopts != '\0') {
            // BSD getsubopt (at least) has undefined behavior on -1, so
            // if we want to retry the getsubopt call in submodules we
            // need an extra layer of string copies.
            char *subopts_temp_o = NULL;
            char *subopts_temp = subopts_temp_o = strdup(subopts);

            switch (getsubopt(&subopts, subopts_tokens, &subopts_value)) {
            case MAXCONNS_FAST:
                printf("설마 여기 들어가나? MAXCONNS_FAST\n");
                settings.maxconns_fast = true;
                break;
            case HASHPOWER_INIT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing numeric argument for hashpower\n");
                    goto error;
                }
                settings.hashpower_init = atoi(subopts_value);
                if (settings.hashpower_init < 12) {
                    fprintf(stderr, "Initial hashtable multiplier of %d is too low\n",
                        settings.hashpower_init);
                    goto error;
                } else if (settings.hashpower_init > 32) {
                    fprintf(stderr, "Initial hashtable multiplier of %d is too high\n"
                        "Choose a value based on \"STAT hash_power_level\" from a running instance\n",
                        settings.hashpower_init);
                    goto error;
                }
                break;
            case NO_HASHEXPAND:
                start_assoc_maint = false;
                break;
            case SLAB_REASSIGN:
                settings.slab_reassign = true;
                break;
            case SLAB_AUTOMOVE:
                if (subopts_value == NULL) {
                    settings.slab_automove = 1;
                    break;
                }
                settings.slab_automove = atoi(subopts_value);
                if (settings.slab_automove < 0 || settings.slab_automove > 2) {
                    fprintf(stderr, "slab_automove must be between 0 and 2\n");
                    goto error;
                }
                break;
            case SLAB_AUTOMOVE_RATIO:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing slab_automove_ratio argument\n");
                    goto error;
                }
                settings.slab_automove_ratio = atof(subopts_value);
                if (settings.slab_automove_ratio <= 0 || settings.slab_automove_ratio > 1) {
                    fprintf(stderr, "slab_automove_ratio must be > 0 and < 1\n");
                    goto error;
                }
                break;
            case SLAB_AUTOMOVE_WINDOW:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing slab_automove_window argument\n");
                    goto error;
                }
                settings.slab_automove_window = atoi(subopts_value);
                if (settings.slab_automove_window < 3) {
                    fprintf(stderr, "slab_automove_window must be > 2\n");
                    goto error;
                }
                break;
            case TAIL_REPAIR_TIME:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing numeric argument for tail_repair_time\n");
                    goto error;
                }
                settings.tail_repair_time = atoi(subopts_value);
                if (settings.tail_repair_time < 10) {
                    fprintf(stderr, "Cannot set tail_repair_time to less than 10 seconds\n");
                    goto error;
                }
                break;
            case HASH_ALGORITHM:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing hash_algorithm argument\n");
                    goto error;
                };
                if (strcmp(subopts_value, "jenkins") == 0) {
                    hash_type = JENKINS_HASH;
                } else if (strcmp(subopts_value, "murmur3") == 0) {
                    hash_type = MURMUR3_HASH;
                } else if (strcmp(subopts_value, "xxh3") == 0) {
                    hash_type = XXH3_HASH;
                } else {
                    fprintf(stderr, "Unknown hash_algorithm option (jenkins, murmur3, xxh3)\n");
                    goto error;
                }
                break;
            case LRU_CRAWLER:
                start_lru_crawler = true;
                break;
            case LRU_CRAWLER_SLEEP:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing lru_crawler_sleep value\n");
                    goto error;
                }
                settings.lru_crawler_sleep = atoi(subopts_value);
                if (settings.lru_crawler_sleep > 1000000 || settings.lru_crawler_sleep < 0) {
                    fprintf(stderr, "LRU crawler sleep must be between 0 and 1 second\n");
                    goto error;
                }
                break;
            case LRU_CRAWLER_TOCRAWL:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing lru_crawler_tocrawl value\n");
                    goto error;
                }
                if (!safe_strtoul(subopts_value, &tocrawl)) {
                    fprintf(stderr, "lru_crawler_tocrawl takes a numeric 32bit value\n");
                    goto error;
                }
                settings.lru_crawler_tocrawl = tocrawl;
                break;
            case LRU_MAINTAINER:
                start_lru_maintainer = true;
                settings.lru_segmented = true;
                break;
            case HOT_LRU_PCT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing hot_lru_pct argument\n");
                    goto error;
                }
                settings.hot_lru_pct = atoi(subopts_value);
                if (settings.hot_lru_pct < 1 || settings.hot_lru_pct >= 80) {
                    fprintf(stderr, "hot_lru_pct must be > 1 and < 80\n");
                    goto error;
                }
                break;
            case WARM_LRU_PCT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing warm_lru_pct argument\n");
                    goto error;
                }
                settings.warm_lru_pct = atoi(subopts_value);
                if (settings.warm_lru_pct < 1 || settings.warm_lru_pct >= 80) {
                    fprintf(stderr, "warm_lru_pct must be > 1 and < 80\n");
                    goto error;
                }
                break;
            case HOT_MAX_FACTOR:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing hot_max_factor argument\n");
                    goto error;
                }
                settings.hot_max_factor = atof(subopts_value);
                if (settings.hot_max_factor <= 0) {
                    fprintf(stderr, "hot_max_factor must be > 0\n");
                    goto error;
                }
                break;
            case WARM_MAX_FACTOR:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing warm_max_factor argument\n");
                    goto error;
                }
                settings.warm_max_factor = atof(subopts_value);
                if (settings.warm_max_factor <= 0) {
                    fprintf(stderr, "warm_max_factor must be > 0\n");
                    goto error;
                }
                break;
            case TEMPORARY_TTL:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing temporary_ttl argument\n");
                    goto error;
                }
                settings.temp_lru = true;
                settings.temporary_ttl = atoi(subopts_value);
                break;
            case IDLE_TIMEOUT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing numeric argument for idle_timeout\n");
                    goto error;
                }
                settings.idle_timeout = atoi(subopts_value);
                break;
            case WATCHER_LOGBUF_SIZE:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing watcher_logbuf_size argument\n");
                    goto error;
                }
                if (!safe_strtoul(subopts_value, &settings.logger_watcher_buf_size)) {
                    fprintf(stderr, "could not parse argument to watcher_logbuf_size\n");
                    goto error;
                }
                settings.logger_watcher_buf_size *= 1024; /* kilobytes */
                break;
            case WORKER_LOGBUF_SIZE:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing worker_logbuf_size argument\n");
                    goto error;
                }
                if (!safe_strtoul(subopts_value, &settings.logger_buf_size)) {
                    fprintf(stderr, "could not parse argument to worker_logbuf_size\n");
                    goto error;
                }
                settings.logger_buf_size *= 1024; /* kilobytes */
            case SLAB_SIZES:
                slab_sizes_unparsed = strdup(subopts_value);
                break;
            case SLAB_CHUNK_MAX:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing slab_chunk_max argument\n");
                    goto error;
                }
                if (!safe_strtol(subopts_value, &settings.slab_chunk_size_max)) {
                    fprintf(stderr, "could not parse argument to slab_chunk_max\n");
                    goto error;
                }
                if (settings.slab_chunk_size_max <= 0) {
                    fprintf(stderr, "slab_chunk_max must be >= 0\n");
                    goto error;
                }
                if (settings.slab_chunk_size_max > (1 << 10)) {
                    fprintf(stderr, "slab_chunk_max must be specified in kilobytes.\n");
                    goto error;
                }
                settings.slab_chunk_size_max *= (1 << 10);
                slab_chunk_size_changed = true;
                break;
            case TRACK_SIZES:
                item_stats_sizes_init();
                break;
            case NO_INLINE_ASCII_RESP:
                break;
            case INLINE_ASCII_RESP:
                break;
            case NO_CHUNKED_ITEMS:
                settings.slab_chunk_size_max = settings.slab_page_size;
                break;
            case NO_SLAB_REASSIGN:
                settings.slab_reassign = false;
                break;
            case NO_SLAB_AUTOMOVE:
                settings.slab_automove = 0;
                break;
            case NO_MAXCONNS_FAST:
                printf("여기 들어가나? NO_MAXCONNS_FAST\n");
                settings.maxconns_fast = false;
                break;
            case NO_LRU_CRAWLER:
                settings.lru_crawler = false;
                start_lru_crawler = false;
                break;
            case NO_LRU_MAINTAINER:
                start_lru_maintainer = false;
                settings.lru_segmented = false;
                break;
#ifdef TLS
            case SSL_CERT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing ssl_chain_cert argument\n");
                    goto error;
                }
                settings.ssl_chain_cert = strdup(subopts_value);
                break;
            case SSL_KEY:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing ssl_key argument\n");
                    goto error;
                }
                settings.ssl_key = strdup(subopts_value);
                break;
            case SSL_VERIFY_MODE:
            {
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing ssl_verify_mode argument\n");
                    goto error;
                }
                int verify  = 0;
                if (!safe_strtol(subopts_value, &verify)) {
                    fprintf(stderr, "could not parse argument to ssl_verify_mode\n");
                    goto error;
                }
                if (!ssl_set_verify_mode(verify)) {
                    fprintf(stderr, "Invalid ssl_verify_mode. Use help to see valid options.\n");
                    goto error;
                }
                break;
            }
            case SSL_KEYFORM:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing ssl_keyformat argument\n");
                    goto error;
                }
                if (!safe_strtol(subopts_value, &settings.ssl_keyformat)) {
                    fprintf(stderr, "could not parse argument to ssl_keyformat\n");
                    goto error;
                }
                break;
            case SSL_CIPHERS:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing ssl_ciphers argument\n");
                    goto error;
                }
                settings.ssl_ciphers = strdup(subopts_value);
                break;
            case SSL_CA_CERT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing ssl_ca_cert argument\n");
                    goto error;
                }
                settings.ssl_ca_cert = strdup(subopts_value);
                break;
            case SSL_WBUF_SIZE:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing ssl_wbuf_size argument\n");
                    goto error;
                }
                if (!safe_strtoul(subopts_value, &settings.ssl_wbuf_size)) {
                    fprintf(stderr, "could not parse argument to ssl_wbuf_size\n");
                    goto error;
                }
                settings.ssl_wbuf_size *= 1024; /* kilobytes */
                break;
            case SSL_SESSION_CACHE:
                settings.ssl_session_cache = true;
                break;
            case SSL_KERNEL_TLS:
                settings.ssl_kernel_tls = true;
                break;
            case SSL_MIN_VERSION: {
                int min_version;
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing ssl_min_version argument\n");
                    goto error;
                }
                if (!safe_strtol(subopts_value, &min_version)) {
                    fprintf(stderr, "could not parse argument to ssl_min_version\n");
                    goto error;
                }
                if (!ssl_set_min_version(min_version)) {
                    fprintf(stderr, "Invalid ssl_min_version. Use help to see valid options.\n");
                    goto error;
                }
                break;
            }
#endif
            case MODERN:
                /* currently no new defaults */
                break;
            case NO_MODERN:
                if (!slab_chunk_size_changed) {
                    settings.slab_chunk_size_max = settings.slab_page_size;
                }
                settings.slab_reassign = false;
                settings.slab_automove = 0;
                settings.maxconns_fast = false;
                settings.lru_segmented = false;
                hash_type = JENKINS_HASH;
                start_lru_crawler = false;
                start_lru_maintainer = false;
                break;
            case NO_DROP_PRIVILEGES:
                settings.drop_privileges = false;
                break;
            case DROP_PRIVILEGES:
                settings.drop_privileges = true;
                break;
            case RESP_OBJ_MEM_LIMIT:
                // TODO: Remove at some point in the future.
                fprintf(stderr, "DEPRECATED: resp_obj_mem_limit no longer used. See read_buf_mem_limit,\n");
                break;
            case READ_BUF_MEM_LIMIT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing read_buf_mem_limit argument\n");
                    goto error;
                }
                if (!safe_strtoul(subopts_value, &settings.read_buf_mem_limit)) {
                    fprintf(stderr, "could not parse argument to read_buf_mem_limit\n");
                    goto error;
                }
                settings.read_buf_mem_limit *= 1024 * 1024; /* megabytes */
                break;
#ifdef PROXY
            case PROXY_CONFIG:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing proxy_config file argument\n");
                    goto error;
                }
                if (protocol_specified) {
                    fprintf(stderr, "Cannot specify a protocol with proxy mode enabled\n");
                    goto error;
                }
                settings.proxy_startfile = strdup(subopts_value);
                settings.proxy_enabled = true;
                settings.binding_protocol = proxy_prot;
                protocol_specified = true;
                break;
            case PROXY_ARG:
                settings.proxy_startarg = strdup(subopts_value);
                break;
            case PROXY_URING:
                settings.proxy_uring = true;
                break;
            case PROXY_MEMPROFILE:
                settings.proxy_memprofile = true;
                break;
#endif
#ifdef MEMCACHED_DEBUG
            case RELAXED_PRIVILEGES:
                settings.relaxed_privileges = true;
                break;
#endif
#ifdef SOCK_COOKIE_ID
            case COOKIE_ID:
                (void)safe_strtoul(subopts_value, &settings.sock_cookie_id);
                break;
#endif
            default:
#ifdef EXTSTORE
                // TODO: differentiating response code.
                if (storage_read_config(storage_cf, &subopts_temp)) {
                    goto error;
                }
#else
#if ENABLE_PRINT
                printf("Illegal suboption \"%s\"\n", subopts_temp);
#endif
                goto error;
#endif
            } // switch
            if (subopts_temp_o) {
                free(subopts_temp_o);
            }

            } // while
            free(subopts_orig);
            break;
        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

    //printf("memcached/memcached.c :: main()-[4]\n");
    if (settings.num_napi_ids > settings.num_threads) {
        fprintf(stderr, "Number of napi_ids(%d) cannot be greater than number of threads(%d)\n",
                settings.num_napi_ids, settings.num_threads);
        exit(EX_USAGE);
    }

    if (settings.item_size_max < ITEM_SIZE_MAX_LOWER_LIMIT) {
        fprintf(stderr, "Item max size cannot be less than 1024 bytes.\n");
        exit(EX_USAGE);
    }
    if (settings.item_size_max > (settings.maxbytes / 2)) {
        fprintf(stderr, "Cannot set item size limit higher than 1/2 of memory max.\n");
        exit(EX_USAGE);
    }
    if (settings.item_size_max > (ITEM_SIZE_MAX_UPPER_LIMIT)) {
        fprintf(stderr, "Cannot set item size limit higher than a gigabyte.\n");
        exit(EX_USAGE);
    }
    if (settings.item_size_max > 1024 * 1024) {
        if (!slab_chunk_size_changed) {
            // Ideal new default is 16k, but needs stitching.
            settings.slab_chunk_size_max = settings.slab_page_size / 2;
        }
    }

    if (settings.slab_chunk_size_max > settings.item_size_max) {
        fprintf(stderr, "slab_chunk_max (bytes: %d) cannot be larger than -I (item_size_max %d)\n",
                settings.slab_chunk_size_max, settings.item_size_max);
        exit(EX_USAGE);
    }

    if (settings.item_size_max % settings.slab_chunk_size_max != 0) {
        fprintf(stderr, "-I (item_size_max: %d) must be evenly divisible by slab_chunk_max (bytes: %d)\n",
                settings.item_size_max, settings.slab_chunk_size_max);
        exit(EX_USAGE);
    }

    if (settings.slab_page_size % settings.slab_chunk_size_max != 0) {
        fprintf(stderr, "slab_chunk_max (bytes: %d) must divide evenly into %d (slab_page_size)\n",
                settings.slab_chunk_size_max, settings.slab_page_size);
        exit(EX_USAGE);
    }
#ifdef EXTSTORE //들어감
    switch (storage_check_config(storage_cf)) { //근데 여기는 안들어감
        case 0:
            storage_enabled = true;
            break;
        case 1:
            exit(EX_USAGE);
            break;
    }
#endif
    // Reserve this for the new default. If factor size hasn't changed, use
    // new default.
    /*if (settings.slab_chunk_size_max == 16384 && settings.factor == 1.25) {
        settings.factor = 1.08;
    }*/
    if (slab_sizes_unparsed != NULL) { //여기 안들어감
        // want the unedited string for restart code.
        char *temp = strdup(slab_sizes_unparsed);
        if (_parse_slab_sizes(slab_sizes_unparsed, slab_sizes)) {
            use_slab_sizes = true;
            if (meta->slab_config) {
                free(meta->slab_config);
            }
            meta->slab_config = temp;
        } else {
            exit(EX_USAGE);
        }
    } else if (!meta->slab_config) { //여기 들어감
        // using the default factor.
        meta->slab_config = "1.25";
    }

    if (settings.hot_lru_pct + settings.warm_lru_pct > 80) {
        fprintf(stderr, "hot_lru_pct + warm_lru_pct cannot be more than 80%% combined\n");
        exit(EX_USAGE);
    }

    if (settings.temp_lru && !start_lru_maintainer) {
        fprintf(stderr, "temporary_ttl requires lru_maintainer to be enabled\n");
        exit(EX_USAGE);
    }

    if (hash_init(hash_type) != 0) {
        fprintf(stderr, "Failed to initialize hash_algorithm!\n");
        exit(EX_USAGE);
    }

    /*
     * Use one workerthread to serve each UDP port if the user specified
     * multiple ports
     */
    if (settings.inter != NULL && strchr(settings.inter, ',')) { //여기 안들어감
        settings.num_threads_per_udp = 1;
    } else {                                                     //여기 들어감          
//#if ENABLE_PRINT
        //printf("memcached/memcached.c :: main()-[10-2] mpi_conn_arr[0]->item : %d\n", mpi_conn_arr[0].item);
//#endif
        settings.num_threads_per_udp = settings.num_threads;
    }

    if (settings.sasl) { //여기 안 들어감
        if (!protocol_specified) {
            settings.binding_protocol = binary_prot;
        } else {
            if (settings.binding_protocol != binary_prot) {
                fprintf(stderr, "ERROR: You cannot allow the ASCII protocol while using SASL.\n");
                exit(EX_USAGE);
            }
        }

        if (settings.udpport) {
            fprintf(stderr, "ERROR: Cannot enable UDP while using binary SASL authentication.\n");
            exit(EX_USAGE);
        }
    }

    if (settings.auth_file) { //여기 안 들어감
        if (!protocol_specified) {
            settings.binding_protocol = ascii_prot;
        } else {
            if (settings.binding_protocol != ascii_prot) {
                fprintf(stderr, "ERROR: You cannot allow the BINARY protocol while using ascii authentication tokens.\n");
                exit(EX_USAGE);
            }
        }
    }

    if (udp_specified && settings.udpport != 0 && !tcp_specified) {
        //여기 안 들어감
        settings.port = settings.udpport;
    }

    if (settings.port > 65535) { //여기 안 들어감
        fprintf(stderr, "ERROR: Invalid port number %d.\n", settings.port);
        exit(EX_USAGE);
    }

    /*
     * Setup SSL if enabled
     */
    if (settings.ssl_enabled) { //여기 안 들어감
        if (!settings.port) {
            fprintf(stderr, "ERROR: You cannot enable SSL without a TCP port.\n");
            exit(EX_USAGE);
        }
        // Initiate the SSL context.
        ssl_init();
    }

    if (maxcore != 0) { //여기 안 들어감
        struct rlimit rlim_new;
        /*
         * First try raising to infinity; if that fails, try bringing
         * the soft limit to the hard.
         */
        if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
            if (setrlimit(RLIMIT_CORE, &rlim_new)!= 0) {
                /* failed. try raising just to the old max */
                rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
                (void)setrlimit(RLIMIT_CORE, &rlim_new);
            }
        }
        /*
         * getrlimit again to see what we ended up with. Only fail if
         * the soft limit ends up 0, because then no core files will be
         * created at all.
         */

        if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0) {
            fprintf(stderr, "failed to ensure corefile creation\n");
            exit(EX_OSERR);
        }
    }
    //printf("memcached/memcached.c :: main()-[8]\n");
    /*
     * If needed, increase rlimits to allow as many connections
     * as needed.
     */

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        fprintf(stderr, "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else { //여기 들어감
        rlim.rlim_cur = settings.maxconns;
        rlim.rlim_max = settings.maxconns;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
#ifndef MEMCACHED_DEBUG
            fprintf(stderr, "failed to set rlimit for open files. Try starting as root or requesting smaller maxconns value.\n");
            exit(EX_OSERR);
#endif
        }
    }

    /* lose root privileges if we have them */
    if (getuid() == 0 || geteuid() == 0) { //여기 안 들어감
        if (username == 0 || *username == '\0') {
            fprintf(stderr, "must add '-u root' to start as root\n");
            exit(EX_USAGE);
        }
        if ((pw = getpwnam(username)) == 0) {
            fprintf(stderr, "can't find the user %s to switch to\n", username);
            exit(EX_NOUSER);
        }
        if (setgroups(0, NULL) < 0) {
            /* setgroups may fail with EPERM, indicating we are already in a
             * minimally-privileged state. In that case we continue. For all
             * other failure codes we exit.
             *
             * Note that errno is stored here because fprintf may change it.
             */
            bool should_exit = errno != EPERM;
            fprintf(stderr, "failed to drop supplementary groups: %s\n",
                    strerror(errno));
            if (should_exit) {
                exit(EX_OSERR);
            }
        }
        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
            fprintf(stderr, "failed to assume identity of user %s\n", username);
            exit(EX_OSERR);
        }
    }

    /* Initialize Sasl if -S was specified */
    if (settings.sasl) { //여기 안 들어감
        init_sasl();
    }
    //printf("memcached/memcached.c :: main()-[9]\n");
    /* daemonize if requested */
    /* if we want to ensure our ability to dump core, don't chdir to / */
    if (do_daemonize) { //여기 안 들어감
        if (daemonize(maxcore, settings.verbose) == -1) {
            fprintf(stderr, "failed to daemon() in order to daemonize\n");
            exit(EXIT_FAILURE);
        }
    }

    /* lock paged memory if needed */
    if (lock_memory) { //여기 안 들어감
#ifdef HAVE_MLOCKALL
        int res = mlockall(MCL_CURRENT | MCL_FUTURE);
        if (res != 0) {
            fprintf(stderr, "warning: -k invalid, mlockall() failed: %s\n",
                    strerror(errno));
        }
#else
        fprintf(stderr, "warning: -k invalid, mlockall() not supported on this platform.  proceeding without.\n");
#endif
    }

#if ENABLE_SOCKET_FUNCTIONS
    /* initialize main thread libevent instance */
#if defined(LIBEVENT_VERSION_NUMBER) && LIBEVENT_VERSION_NUMBER >= 0x02000101
    //여기 들어감
    /* If libevent version is larger/equal to 2.0.2-alpha, use newer version */
    struct event_config *ev_config;
    ev_config = event_config_new(); //event_base 객체를 위해, event 설정 객체를 생성한다.
    event_config_set_flag(ev_config, EVENT_BASE_FLAG_NOLOCK);  //이벤트 루프(event base)를 만들기 전에 스레드 락을 쓰지 않게 막는 용도
    main_base = event_base_new_with_config(ev_config); //얘도 event_base 생성하는 함수
    //event base가 뭔가 .. : 등록된 이벤트들을 관리하고, 이벤트가 발생하면 그에 대한 콜백함수를 직접 실행해주는 컨트롤러 역할을 함
    event_config_free(ev_config);
#else
    /* Otherwise, use older API */
    main_base = event_init(); //event_base를 초기화하는 함수
#endif
#endif

    /* Load initial auth file if required */
    //여기 안 들어감
    if (settings.auth_file) {
        if (settings.udpport) {
            fprintf(stderr, "Cannot use UDP with ascii authentication enabled (-U 0 to disable)\n");
            exit(EX_USAGE);
        }

        switch (authfile_load(settings.auth_file)) {
            case AUTHFILE_STATFAIL:
                vperror("Could not stat authfile [%s], error %s", settings.auth_file
                                                            , strerror(errno));
                exit(EXIT_FAILURE);
                break;
            case AUTHFILE_OPENFAIL:
                vperror("Could not open authfile [%s] for reading, error %s", settings.auth_file
                                                                           , strerror(errno));
                exit(EXIT_FAILURE);
                break;
            case AUTHFILE_OOM:
                fprintf(stderr, "Out of memory reading password file: %s", settings.auth_file);
                exit(EXIT_FAILURE);
                break;
            case AUTHFILE_MALFORMED:
                fprintf(stderr, "Authfile [%s] has a malformed entry. Should be 'user:password'", settings.auth_file);
                exit(EXIT_FAILURE);
                break;
            case AUTHFILE_OK:
                break;
        }
    }

/**************************************************************************************************************************************** */
    //여기서부터 진짜 network도 초기화하고~그러는듯
    //printf("memcached/memcached.c :: main()-[13]\n");
    /* initialize other stuff */
    stats_init();
    logger_init();
    logger_create(); // main process logger
#if ENABLE_SOCKET_FUNCTIONS
    conn_init();
#endif
    bool reuse_mem = false;
    void *mem_base = NULL;
    bool prefill = false;
    if (settings.memory_file != NULL) { //여기 안 들어감
        preallocate = true;
        // Easier to manage memory if we prefill the global pool when reusing.
        prefill = true;
        restart_register("main", _mc_meta_load_cb, _mc_meta_save_cb, meta);
        reuse_mem = restart_mmap_open(settings.maxbytes,
                        settings.memory_file,
                        &mem_base);
        // The "save" callback gets called when we're closing out the mmap,
        // but we don't know what the mmap_base is until after we call open.
        // So we pass the struct above but have to fill it in here so the
        // data's available during the save routine.
        meta->mmap_base = mem_base;
        // Also, the callbacks for load() run before _open returns, so we
        // should have the old base in 'meta' as of here.
    }
    // Initialize the hash table _after_ checking restart metadata.
    // We override the hash table start argument with what was live
    // previously, to avoid filling a huge set of items into a tiny hash
    // table.
    // restart metadata를 확인한 이후 해시테이블을 초기화한다.
    // 이전에 살아있던 데이터를 기준으로 해시테이블 초기 인자를 덮어쓴다.
    // 많은 아이템들을 작은 해시테이블에 넣는 일을 피하기 위함임.
    assoc_init(settings.hashpower_init);

#ifdef EXTSTORE //둘다 0이라 안들어감
    if (storage_enabled && reuse_mem) {
        fprintf(stderr, "[restart] memory restart with extstore not presently supported.\n");
        reuse_mem = false;
    }
#endif
    slabs_init(settings.maxbytes, settings.factor, preallocate,
            use_slab_sizes ? slab_sizes : NULL, mem_base, reuse_mem);
#ifdef EXTSTORE
    if (storage_enabled) { //storage_enabled가 0이라 안들어감
        storage = storage_init(storage_cf);
        if (storage == NULL) {
            exit(EXIT_FAILURE);
        }
        ext_storage = storage;
        /* page mover algorithm for extstore needs memory prefilled */
        prefill = true;
    }
#endif
    if (settings.drop_privileges) { //여기도 안 들어감
        setup_privilege_violations_handler();
    }
    if (prefill) //prefill도 0이라, 안들어감..
        slabs_prefill_global();
    /* In restartable mode and we've decided to issue a fixup on memory */
    if (settings.memory_file != NULL && reuse_mem) { //여기도 안 들어감
        mc_ptr_t old_base = meta->old_base;
        assert(old_base == meta->old_base);

        // should've pulled in process_started from meta file.
        process_started = meta->process_started;
        // TODO: must be a more canonical way of serializing/deserializing
        // pointers? passing through uint64_t should work, and we're not
        // annotating the pointer with anything, but it's still slightly
        // insane.
        restart_fixup((void *)old_base);
    }
    /*
     * ignore SIGPIPE signals; we can use errno == EPIPE if we
     * need that information
     */
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        perror("failed to ignore SIGPIPE; sigaction");
        exit(EX_OSERR);
    }
    /* start up worker threads if MT mode */
#ifdef PROXY
    if (settings.proxy_enabled) {
        settings.proxy_ctx = proxy_init(settings.proxy_uring, settings.proxy_memprofile);
    }
#endif
#ifdef EXTSTORE
    //pthread_t temp_tid = pthread_self();
    //printf("[%lu] memcached_thread_init() 호출\n", temp_tid);

    memcached_thread_init(settings.num_threads, storage);
    init_lru_crawler(storage);
#else
    memcached_thread_init(settings.num_threads, NULL);
    init_lru_crawler(NULL);
#endif

#ifdef PROXY
    if (settings.proxy_enabled) {
        if (proxy_first_confload(settings.proxy_ctx) != 0) {
            exit(EXIT_FAILURE);
        }
    }
#endif
    if (start_assoc_maint && start_assoc_maintenance_thread() == -1) {
        exit(EXIT_FAILURE);
    }
    if (start_lru_crawler && start_item_crawler_thread() != 0) {
        fprintf(stderr, "Failed to enable LRU crawler thread\n");
        exit(EXIT_FAILURE);
    }
#ifdef EXTSTORE
    if (storage && start_storage_compact_thread(storage) != 0) {
        fprintf(stderr, "Failed to start storage compaction thread\n");
        exit(EXIT_FAILURE);
    }
    if (storage && start_storage_write_thread(storage) != 0) {
        fprintf(stderr, "Failed to start storage writer thread\n");
        exit(EXIT_FAILURE);
    }
    if (start_lru_maintainer && start_lru_maintainer_thread(storage) != 0) {
#else
    if (start_lru_maintainer && start_lru_maintainer_thread(NULL) != 0) {
#endif
        fprintf(stderr, "Failed to enable LRU maintainer thread\n");
        free(meta);
        return 1;
    }
#if ENABLE_PRINT
     printf("memcached/memcached.c :: main()-[14]-5\n");
#endif
    if (settings.slab_reassign) {
        settings.slab_rebal = start_slab_maintenance_thread(storage);
        if (!settings.slab_rebal) {
            exit(EXIT_FAILURE);
        }
    }

    if (settings.idle_timeout && start_conn_timeout_thread() == -1) {
        exit(EXIT_FAILURE);
    }
#if ENABLE_PRINT
     printf("memcached/memcached.c :: main()-[14]-6\n");
#endif
    /* initialise clock event */
#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
    {
#if ENABLE_PRINT
        printf("memcached/memcached.c :: main()-[14]-6-1\n");
#endif
        struct timespec ts;
        if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
#if ENABLE_PRINT
            printf("memcached/memcached.c :: main()-[14]-6-2\n");
#endif
            monotonic = true;
            monotonic_start = ts.tv_sec;
            // Monotonic clock needs special handling for restarts.
            // We get a start time at an arbitrary place, so we need to
            // restore the original time delta, which is always "now" - _start
            if (reuse_mem) {
#if ENABLE_PRINT
                printf("memcached/memcached.c :: main()-[14]-6-3\n");
#endif
                // the running timespan at stop time + the time we think we
                // were stopped.
                monotonic_start -= meta->current_time + meta->time_delta;
            } else {
#if ENABLE_PRINT
                printf("memcached/memcached.c :: main()-[14]-6-4\n");
#endif
                monotonic_start -= ITEM_UPDATE_INTERVAL + 2;
            }
        }
    }
#endif
#if ENABLE_PRINT
    printf("memcached/memcached.c :: main()-[14]-6-5\n");
#endif

#if ENABLE_SOCKET_FUNCTIONS
    clock_handler(0, 0, 0);
#endif
#if ENABLE_MPI_FUNCTIONS
    mpi_clock_handler(0, 0, 0);
#endif
    //printf("memcached/memcached.c :: main()-[17]\n");
    /* create unix mode sockets after dropping privileges */
#if ENABLE_PRINT
    printf("memcached/memcached.c :: main()-[17] settings.socketpath : %d\n", settings.socketpath);
#endif
    if (settings.socketpath != NULL) { //여기는 안들어감
        //printf("memcached/memcached.c :: main()-[18], settings.socketpath is not NULL\n");
        errno = 0;
        if (server_socket_unix(settings.socketpath,settings.access)) {
            vperror("failed to listen on UNIX socket: %s", settings.socketpath);
            exit(EX_OSERR);
        }
    }
    //printf("memcached/memcached.c :: main()-[14]-6\n");
    /* create the listening socket, bind it, and init */
    if (settings.socketpath == NULL) {
        const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
#if ENABLE_PRINT
        printf("\t portnumber_filename : %s\n", portnumber_filename);
#endif
        ////printf("memcached/memcached.c :: main() - settings.socketpath==NULL\n portnumber_filename : %s\n", portnumber_filename);
        char *temp_portnumber_filename = NULL;
        size_t len;
        FILE *portnumber_file = NULL;
        //printf("memcached/memcached.c :: main()-[18]-1\n");
        if (portnumber_filename != NULL) { //여기는 portnumber_filename가 0이라서 안들어감
            len = strlen(portnumber_filename)+4+1;
            temp_portnumber_filename = malloc(len);
            if (temp_portnumber_filename == NULL) {
                vperror("Failed to allocate memory for portnumber file");
                exit(EX_OSERR);
            }
            snprintf(temp_portnumber_filename,
                     len,
                     "%s.lck", portnumber_filename);

            portnumber_file = fopen(temp_portnumber_filename, "a");
            if (portnumber_file == NULL) {
                fprintf(stderr, "Failed to open \"%s\": %s\n",
                        temp_portnumber_filename, strerror(errno));
            }
        }

        errno = 0;
#if ENABLE_PRINT
        printf("memcached/memcached.c :: main()-[19], server_sockets call, port : %d, tcp_transport : %d\n", settings.port, tcp_transport);
#endif
        #if ENABLE_SOCKET_FUNCTIONS
        if (settings.port && server_sockets(settings.port, tcp_transport, portnumber_file)) {
            //printf("memcached/memcached.c :: main()-[19]-1\n");
            if (settings.inter == NULL) {
                vperror("failed to listen on TCP port %d", settings.port);
            } else {
                vperror("failed to listen on one of interface(s) %s", settings.inter);
            }
            free(temp_portnumber_filename);
            exit(EX_OSERR);
        }
        #endif

        /***************************************0711 여기부터 다시***************************************** */
        /*
         * initialization order: first create the listening sockets
         * (may need root on low ports), then drop root if needed,
         * then daemonize if needed, then init libevent (in some cases
         * descriptors created by libevent wouldn't survive forking).

            * 초기화 순서:
            * 먼저 리스닝 소켓(listening sockets)을 생성합니다  
            *  (낮은 포트 번호를 사용할 경우 root 권한이 필요할 수 있음),
            * 그 다음 필요하다면 root 권한을 내려놓고(drop root),
            * 필요하다면 데몬화(daemonize)를 수행한 후,
            * 마지막으로 libevent를 초기화(init libevent)합니다  
            *  (어떤 경우에는 libevent가 생성한 디스크립터들이  
            *   fork 이후에도 유지되지 않을 수 있기 때문입니다).
         */

        /* create the UDP listening socket and bind it */
        errno = 0;
        //printf("memcached/memcached.c :: main()-[20], server_sockets call, udpport : %d, udp_transport : %d\n", settings.udpport, udp_transport);
        if (settings.udpport && server_sockets(settings.udpport, udp_transport, portnumber_file)) {
            //printf("memcached/memcached.c :: main()-[20], server_sockets call(udp)\n");
            if (settings.inter == NULL) {
                vperror("failed to listen on UDP port %d", settings.udpport);
            } else {
                vperror("failed to listen on one of interface(s) %s", settings.inter);
            }
            free(temp_portnumber_filename);
            exit(EX_OSERR);
        }

        if (portnumber_file) {
            fclose(portnumber_file);
            rename(temp_portnumber_filename, portnumber_filename);
        }
        if (temp_portnumber_filename)
            free(temp_portnumber_filename);
    }
#if ENABLE_PRINT
     printf("memcached/memcached.c :: main()-[14]-7\n");
#endif
    /* Give the sockets a moment to open. I know this is dumb, but the error
     * is only an advisory.
     */
    usleep(1000);
    if (stats_state.curr_conns + stats_state.reserved_fds >= settings.maxconns - 1) {
        fprintf(stderr, "Maxconns setting is too low, use -c to increase.\n");
        exit(EXIT_FAILURE);
    }

    if (pid_file != NULL) {
        save_pid(pid_file);
    }  
#if ENABLE_PRINT
     printf("memcached/memcached.c :: main()-[14]-8\n");
#endif
    /* Drop privileges no longer needed */
    if (settings.drop_privileges) {
        drop_privileges();
    }
#if ENABLE_PRINT
    printf("memcached/memcached.c :: main()-[14]-9\n");
#endif
    /* Initialize the uriencode lookup table. */
    uriencode_init();
// #if ENABLE_PRINT
//      printf("memcached/memcached.c :: main()-[14]-10, mpi_conn_arr[0]->item : %d\n", mpi_conn_arr[0].item);
// #endif
    /* enter the event loop */

    pthread_t ttid = pthread_self(); 
    //sleep(5);
    // char ch;
    // printf("getchar : ");
    // ch=getchar();
    #if ENABLE_PRINT
        printf("memcached/memcached.c :: main()-[14]-10\n");
    #endif
    //printf("server debugging1\n");
#if ENABLE_MPI_FUNCTIONS
    while (!stop_main_loop) { //여기를 계속 들어가네 ..
        while(true){
#if MEMCACHED_MODE
        mpi_drive_machine(&mpi_conn_arr[0], ttid);
#endif

#if PS_MODE
        mpi_drive_machine(&mpi_conn_arr[rank-1], ttid);
#endif
        }
    }
#endif


#if ENABLE_SOCKET_FUNCTIONS
    while (!stop_main_loop) { //여기를 계속 들어가네 ..
        if (event_base_loop(main_base, EVLOOP_ONCE) != 0) { //얘는 한번만 발생하는 루프
            retval = EXIT_FAILURE;
            break;
        }
    }
#if ENABLE_PRINT
    printf("memcached/memcached.c :: main()-[14]-11\n");
#endif
    switch (stop_main_loop) {
        case GRACE_STOP:
            fprintf(stderr, "Gracefully stopping\n");
        break;
        case EXIT_NORMALLY:
            // Don't need to print anything to STDERR for a normal shutdown except
            // if we want to.

            if (settings.verbose) {
                fprintf(stderr, "Exiting normally\n");
            }

        break;
        default:
            fprintf(stderr, "Exiting on error\n");
        break;
    }
#if ENABLE_PRINT
    printf("memcached/memcached.c :: main()-[14]-12\n");
#endif
    if (stop_main_loop == GRACE_STOP) {
        stop_threads();
        if (settings.memory_file != NULL) {
            restart_mmap_close();
        }
    }
#endif

    /* remove the PID file if we're a daemon */
    if (do_daemonize)
        remove_pidfile(pid_file);
    /* Clean up strdup() call for bind() address */
    if (settings.inter)
      free(settings.inter);

#if ENABLE_SOCKET_FUNCTIONS
    /* cleanup base */
    event_base_free(main_base);
#endif

    free(meta);

    return retval;

error:
    if (subopts_orig)
        free(subopts_orig);
    return 1;
}
