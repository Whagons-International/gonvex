#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
/* Linux CI fault injection: delay only the background checkpointer's disk
 * syncs. All sync calls still execute. The ordinary WAL-bound integration
 * test must pass with this interposer; without the shared checkpoint guard
 * it reproduces the cap overrun seen on slower hosted-runner disks. */
static pthread_once_t symbols = PTHREAD_ONCE_INIT;
static int (*original_fdatasync)(int);
static int (*original_fsync)(int);
static void load_symbols(void) {
  original_fdatasync=dlsym(RTLD_NEXT,"fdatasync");
  original_fsync=dlsym(RTLD_NEXT,"fsync");
}
static void checkpoint_delay(void) {
  char name[16] = {0};
  pthread_getname_np(pthread_self(), name, sizeof(name));
  if (strncmp(name, "gonvex-sqlite", 13) == 0) {
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
