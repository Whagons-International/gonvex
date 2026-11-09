#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
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
static int durability_test;
static void load_symbols(void) {
  original_fdatasync=dlsym(RTLD_NEXT,"fdatasync");
  original_fsync=dlsym(RTLD_NEXT,"fsync");
  char executable[4096];
  ssize_t length=readlink("/proc/self/exe",executable,sizeof(executable)-1);
  if (length>0) {
    executable[length]=0;
    durability_test=strstr(executable,"/writer_durability-")!=0;
  }
}
static void checkpoint_delay(void) {
  if (!durability_test) return;
  char name[16] = {0};
  pthread_getname_np(pthread_self(), name, sizeof(name));
  if (strncmp(name, "gonvex-sqlite", 13) == 0 || strncmp(name, "tokio-runtime", 13) == 0) {
    struct timespec delay = {.tv_sec=0,.tv_nsec=100000000};
    nanosleep(&delay, 0);
  }
}
int fdatasync(int fd) {
  pthread_once(&symbols, load_symbols);
  checkpoint_delay();
  return original_fdatasync(fd);
}
int fsync(int fd) {
  pthread_once(&symbols, load_symbols);
  checkpoint_delay();
  return original_fsync(fd);
}
