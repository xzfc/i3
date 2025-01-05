/*
 * vim:ts=4:sw=4:expandtab
 *
 * i3 - an improved tiling window manager
 * © 2009 Michael Stapelberg and contributors (see also: LICENSE)
 *
 * display_version.c: displays the running i3 version, runs as part of
 *                    i3 --moreversion.
 *
 */
#include "all.h"

#include <fcntl.h>
#include <time.h>
#include <unistd.h>

#include <json-c/json_object.h>

static void print_config_path(const char *path, const char *role) {
    struct stat sb;
    time_t now;
    char mtime[64];

    printf("  %s (%s)", path, role);
    if (stat(path, &sb) == -1) {
        printf("\n");
        ELOG("Cannot stat config file \"%s\"\n", path);
    } else {
        strftime(mtime, sizeof(mtime), "%c", localtime(&(sb.st_mtime)));
        time(&now);
        printf(" (last modified: %s, %.f seconds ago)\n", mtime, difftime(now, sb.st_mtime));
    }
}

/*
 * Connects to i3 to find out the currently running version. Useful since it
 * might be different from the version compiled into this binary (maybe the
 * user didn’t correctly install i3 or forgot to restart it).
 *
 * The output looks like this:
 * Running i3 version: 4.2-202-gb8e782c (2012-08-12, branch "next") (pid 14804)
 *
 * The i3 binary you just called: /home/michael/i3/i3
 * The i3 binary you are running: /home/michael/i3/i3
 *
 */
void display_running_version(void) {
    if (getenv("DISPLAY") == NULL) {
        fprintf(stderr, "\nYour DISPLAY environment variable is not set.\n");
        fprintf(stderr, "Are you running i3 via SSH or on a virtual console?\n");
        fprintf(stderr, "Try DISPLAY=:0 i3 --moreversion\n");
        exit(EXIT_FAILURE);
    }

    char *pid_from_atom = root_atom_contents("I3_PID", conn, conn_screen);
    if (pid_from_atom == NULL) {
        /* If I3_PID is not set, the running version is older than 4.2-200. */
        printf("\nRunning version: < 4.2-200\n");
        exit(EXIT_SUCCESS);
    }

    /* Inform the user of what we are doing. While a single IPC request is
     * really fast normally, in case i3 hangs, this will not terminate. */
    printf("(Getting version from running i3, press ctrl-c to abort…)");
    fflush(stdout);

    int sockfd = ipc_connect(NULL);
    if (ipc_send_message(sockfd, 0, I3_IPC_MESSAGE_TYPE_GET_VERSION,
                         (uint8_t *)"") == -1) {
        err(EXIT_FAILURE, "IPC: write()");
    }

    uint32_t reply_length;
    uint32_t reply_type;
    uint8_t *reply;
    int ret;
    if ((ret = ipc_recv_message(sockfd, &reply_type, &reply_length, &reply)) != 0) {
        if (ret == -1) {
            err(EXIT_FAILURE, "IPC: read()");
        }
        exit(EXIT_FAILURE);
    }

    if (reply_type != I3_IPC_MESSAGE_TYPE_GET_VERSION) {
        errx(EXIT_FAILURE, "Got reply type %d, but expected %d (GET_VERSION)", reply_type, I3_IPC_MESSAGE_TYPE_GET_VERSION);
    }

    json_object *obj = json_parse(reply, reply_length), *obj_field;
    if (!obj) {
        errx(EXIT_FAILURE, "Could not parse my own reply. That's weird. reply is %.*s", (int)reply_length, reply);
    }

    const char *human_readable_version =
        json_object_object_get_ex(obj, "human_readable", &obj_field)
            ? json_object_get_string(obj_field)
            : NULL;

    printf("\r\x1b[K");
    printf("Running i3 version: %s (pid %s)\n", human_readable_version, pid_from_atom);

    if (json_object_object_get_ex(obj, "loaded_config_file_name", &obj_field)) {
        printf("Loaded i3 config:\n");
        print_config_path(json_object_get_string(obj_field), "main");

        if (json_object_object_get_ex(obj, "included_config_file_names", &obj_field)) {
            for (size_t i = 0; i < json_object_array_length(obj_field); i++) {
                print_config_path(json_object_get_string(json_object_array_get_idx(obj_field, i)),
                                  "included");
            }
        }
    }

#ifdef __linux__
    size_t destpath_size = 1024;
    ssize_t linksize;
    char *exepath;
    char *destpath = smalloc(destpath_size);

    sasprintf(&exepath, "/proc/%d/exe", getpid());

    while ((linksize = readlink(exepath, destpath, destpath_size)) == (ssize_t)destpath_size) {
        destpath_size = destpath_size * 2;
        destpath = srealloc(destpath, destpath_size);
    }
    if (linksize == -1) {
        err(EXIT_FAILURE, "readlink(%s)", exepath);
    }

    /* readlink() does not NULL-terminate strings, so we have to. */
    destpath[linksize] = '\0';

    printf("\n");
    printf("The i3 binary you just called: %s\n", destpath);

    free(exepath);
    sasprintf(&exepath, "/proc/%s/exe", pid_from_atom);

    while ((linksize = readlink(exepath, destpath, destpath_size)) == (ssize_t)destpath_size) {
        destpath_size = destpath_size * 2;
        destpath = srealloc(destpath, destpath_size);
    }
    if (linksize == -1) {
        err(EXIT_FAILURE, "readlink(%s)", exepath);
    }

    /* readlink() does not NULL-terminate strings, so we have to. */
    destpath[linksize] = '\0';

    /* Check if "(deleted)" is the readlink result. If so, the running version
     * does not match the file on disk. */
    if (strstr(destpath, "(deleted)") != NULL) {
        printf("RUNNING BINARY DIFFERENT FROM BINARY ON DISK!\n");
    }

    /* Since readlink() might put a "(deleted)" somewhere in the buffer and
     * stripping that out seems hackish and ugly, we read the process’s argv[0]
     * instead. */
    free(exepath);
    sasprintf(&exepath, "/proc/%s/cmdline", pid_from_atom);

    int fd;
    if ((fd = open(exepath, O_RDONLY)) == -1) {
        err(EXIT_FAILURE, "open(%s)", exepath);
    }
    if (read(fd, destpath, sizeof(destpath)) == -1) {
        err(EXIT_FAILURE, "read(%s)", exepath);
    }
    close(fd);

    printf("The i3 binary you are running: %s\n", destpath);

    free(exepath);
    free(destpath);
#endif

    json_object_put(obj);
    free(reply);
    free(pid_from_atom);
}
