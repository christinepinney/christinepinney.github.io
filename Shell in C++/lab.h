/*Do NOT modify this file! */
#ifndef LAB_H
#define LAB_H
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include <termios.h>
#include <unistd.h>
#include <lab_export.h>

#define UNUSED(x) (void)x;

#ifdef __cplusplus
extern "C"
{
#endif

    /* Struct for shell */
    typedef struct shell
    {
        pid_t shell_pgid;
        struct termios shell_tmodes;
        int shell_terminal;
        int shell_is_interactive;
    } shell;

    /* A process is a single process.  */
    typedef struct process
    {
        struct process *next;       /* next process in pipeline */
        char **argv;                /* for exec */
        pid_t pid;                  /* process ID */
        char completed;             /* true if process has completed */
        char stopped;               /* true if process has stopped */
        int status;                 /* reported status value */
    } process;

    /* A job is a pipeline of processes (or just one process in this case).  */
    typedef struct job
    {
        struct job *next;           /* next active job */
        char *command;              /* command line, used for messages */
        process *first_process;     /* list of processes in this job */
        pid_t pgid;                 /* process group ID */
        char notified;              /* true if user told about stopped job */
        int jnum;                   /* job number if background job*/
    } job;

    /**
    * @brief Initialize and return shell. Function built from glibc manual
    *        example.
    *
    * @return shell* The shell
    */
    LAB_EXPORT shell* init_shell();

    /**
    * @brief Initialize and exec() process. Function built from glibc manual
    *        example.
    *
    * @param char** The command for exec() from readline() 
    * @param pid_t The process group id for process
    * @param shell The shell where process exists
    * @param int Indicates whether process if in foreground or background
    */
    LAB_EXPORT void init_process(char **cmd, pid_t pgid, shell *newShell, int foreground);

    /**
    * @brief Utility function for job status. Function built from glibc manual
    *        example.
    *
    * @param job The job to check status of 
    * @return int returns 0 if job process is completed, 1 if not completed
    */
    LAB_EXPORT int job_is_completed (job *j);

    /**
    * @brief Utility function for job status. Function built from glibc manual
    *        example.
    *
    * @param job The job to check status of
    * @return int returns 0 if job process is completed and stopped, 1 if not 
    *         completed and stopped.
    */
    LAB_EXPORT int job_is_stopped (job *j);

    /**
    * @brief Wait for job to finish if running in foreground. Function built
    *        from glibc manual example.
    *
    * @param job The job to wait for
    */
    LAB_EXPORT void wait_for_job (job *j);

    /**
    * @brief Put job in foreground. Function built from glibc manual example.
    *
    * @param job The job to put in foreground
    * @param int Indicates whether job needs to be continued
    * @param shell The shell where job exists
    */
    LAB_EXPORT void put_job_in_foreground (job *j, int cont, shell *newShell);
    
    /**
    * @brief Put job in background. Function built from glibc manual example.
    *
    * @param job The job to put in background
    * @param int Indicates whether job needs to be continued
    */
    LAB_EXPORT void put_job_in_background (job *j, int cont);
    
    /**
    * @brief Initialize job. Function built from glibc manual example.
    *
    * @param char** The command to exec() in process
    * @param job The job to put in foreground
    * @param int Indicates whether job should be in foreground or background
    * @param shell The shell where job exists
    */
    LAB_EXPORT void init_job(char **cmd, job *j, int foreground, shell *newShell);

    /**
     * @brief Set the shell prompt. This function will attempt to load a prompt
     * from the requested environment variable, if the environment variable is
     * not set a default prompt of "shell>" is returned.  This function calls
     * malloc internally and the caller must free the resulting string.
     *
     * @param env The environment variable
     * @return const char* The prompt
     */
    LAB_EXPORT char *get_prompt(const char *env);

    /**
     * Changes the current working directory of the shell. Uses the linux system
     * call chdir. With no arguments the users home directory is used as the
     * directory to change to.
     *
     * @param dir The directory to change to
     * @return  On success, zero is returned.  On error, -1 is returned, and
     * errno is set to indicate the error.
     */
    LAB_EXPORT int change_dir(char **dir);

    /**
     * @brief Convert line read from the user into to format that will work with
     * execvp We limit the number of arguments to ARG_MAX loaded from sysconf.
     * This function allocates memory that must be reclaimed with the cmd_free
     * function.
     *
     * @param line The line to process
     *
     * @return The line read in a format suitable for exec
     */
    LAB_EXPORT char **cmd_parse(char const *line);

    /**
     * @brief Free the line that was constructed with parse_cmd
     *
     * @param line the line to free
     */
    LAB_EXPORT void cmd_free(char ** line);

    /**
     * @brief Trim the whitespace from the start and end of a string.
     * For example "   ls -a   " becomes "ls -a". This function modifies
     * the argument line so that all printable chars are moved to the
     * front of the string
     *
     * @param line The line to trim
     * @return The new line with no whitespace
     */
    LAB_EXPORT char *trim_white(char *line);

    /**
     * @brief Entry point for the main function
     *
     * @param argc The argument count
     * @param argv The argument array
     * @return The exit code
     */
    LAB_EXPORT int go(int argc, char **argv);

#ifdef __cplusplus
} // extern "C"
#endif

#endif
