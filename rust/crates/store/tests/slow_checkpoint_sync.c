#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
/* Linux CI fault injection, scoped to the writer_durability test executable.
 * Delay background checkpoint and group-commit syncs so concurrent work
 * overlaps on fast disks too. Every sync still executes. This exercises the
 * unchanged WAL-cap and shared-sync assertions under controlled contention. */
static pthread_once_t symbols = PTHREAD_ONCE_INIT;
static int (*original_fdatasync)(int);
static int (*original_fsync)(int);
static int (*original_fdatasync64)(int);
static int durability_test;
static _Atomic unsigned long wal_delays;
static _Atomic unsigned long checkpoint_delays;
static void load_symbols(void) {
  original_fdatasync=dlsym(RTLD_NEXT,"fdatasync");
  original_fsync=dlsym(RTLD_NEXT,"fsync");
  original_fdatasync64=dlsym(RTLD_NEXT,"fdatasync64");
  char executable[4096];
  ssize_t length=readlink("/proc/self/exe",executable,sizeof(executable)-1);
  if (length>0) {
    executable[length]=0;
    durability_test=strstr(executable,"/writer_durability-")!=0;
  }
}
static void checkpoint_delay(int fd) {
  if (!durability_test) return;
  char name[16] = {0};
  pthread_getname_np(pthread_self(), name, sizeof(name));
  char link[64],path[4096];
  snprintf(link,sizeof(link),"/proc/self/fd/%d",fd);
  ssize_t length=readlink(link,path,sizeof(path)-1);
  int wal=length>=4 && memcmp(path+length-4,"-wal",4)==0;
  int checkpoint=strncmp(name,"gonvex-sqlite",13)==0;
  if (wal || checkpoint) {
    if (wal) atomic_fetch_add(&wal_delays,1);
    if (checkpoint) atomic_fetch_add(&checkpoint_delays,1);
    struct timespec delay = {.tv_sec=0,.tv_nsec=100000000};
    nanosleep(&delay, 0);
  }
}
__attribute__((destructor)) static void report_delays(void) {
  if (durability_test) fprintf(stderr,"gonvex-writer-io-fixture: wal_delays=%lu checkpoint_delays=%lu\n",atomic_load(&wal_delays),atomic_load(&checkpoint_delays));
}
int fdatasync(int fd) {
  pthread_once(&symbols, load_symbols);
  checkpoint_delay(fd);
  return original_fdatasync(fd);
}
int fsync(int fd) {
  pthread_once(&symbols, load_symbols);
  checkpoint_delay(fd);
  return original_fsync(fd);
}
int fdatasync64(int fd) {
  pthread_once(&symbols, load_symbols);
  checkpoint_delay(fd);
  return original_fdatasync64 ? original_fdatasync64(fd) : original_fdatasync(fd);
}
