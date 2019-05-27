#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#include <signal.h>
#include <errno.h>
#include <limits.h>

enum
  {
   LAYER_MODE_METADATA  = 1 << 0,
   LAYER_MODE_DIRECTORY = 1 << 1,
   LAYER_MODE_FILE      = 1 << 2,
  };

struct context
{
  int dirfd;
  int workdirfd;
  int pid;
  int sockfd;
};

int
plugin_version ()
{
  return 1;
}

const char *
plugin_name ()
{
  return "crfs";
}

void *
plugin_init (const char *data, const char *workdir, int workdirfd, const char *target, int dirfd)
{
  struct context *ctx = NULL;
  int sockfd[2];
  pid_t pid;

  if (socketpair (AF_LOCAL, SOCK_DGRAM, 0, sockfd) < 0)
    return NULL;

  pid = fork ();
  if (pid < 0)
    {
      close (sockfd[0]);
      close (sockfd[1]);
      return NULL;
    }
  if (pid == 0)
    {
      char f[12];

      close (sockfd[0]);
      sprintf (f, "%d", sockfd[1]);
      setenv ("_SOCKFD", f, 1);
      prctl (PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
      execlp ("crfs", "crfs", data, workdir, target, NULL);
      fprintf (stderr, "cannot exec crfs: %s\n", strerror (errno));
      fclose (stderr);
      _exit (EXIT_FAILURE);
    }

  close (sockfd[1]);

  ctx  = malloc (sizeof *ctx);
  ctx->dirfd = dirfd;
  ctx->workdirfd = workdirfd;
  ctx->pid = pid;
  ctx->sockfd = sockfd[0];

  return ctx;
}

int
plugin_fetch (void *opaque, const char *parentdir, const char *path, int mode)
{
  struct context *ctx = opaque;
  char buffer[PATH_MAX*2+16];
  int written;
  int len = sprintf (buffer, "%s:%s:%d", parentdir, path, mode);
  
  do
    written = write (ctx->sockfd, buffer, len);
  while (written < 0 && errno == EINTR);
  if (written < 0)
    return -1;

  do
    len = read (ctx->sockfd, buffer, sizeof buffer);
  while (len < 0 && errno == EINTR);

  if (len > 0 && buffer[0] == '0')
    return 0;

  return -1;
}

int
plugin_release (void *opaque)
{
  struct context *ctx = opaque;

  close (ctx->sockfd);
  free (ctx);
}
