#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <sys/types.h>
#include <termios.h>
#include <unistd.h>
#include <signal.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <pwd.h>
#include <sys/wait.h>
#include "lab.h"
#include "labconfig.h"

shell* init_shell()  {
    shell *myShell = (shell*) malloc(sizeof(shell));
    memset(myShell, 0, sizeof(shell));
    myShell->shell_terminal = STDIN_FILENO;
    myShell->shell_is_interactive = isatty(myShell->shell_terminal);
    myShell->shell_pgid = getpgrp();
    if (myShell->shell_is_interactive)
    {
        while (tcgetpgrp (myShell->shell_terminal) != (myShell->shell_pgid))
        {
            kill (- myShell->shell_pgid, SIGTTIN);
        }
        // ignore signals
        signal(SIGINT, SIG_IGN);
        signal(SIGQUIT, SIG_IGN);
        signal(SIGTSTP, SIG_IGN);
        signal(SIGTTIN, SIG_IGN);
        signal(SIGTTOU, SIG_IGN);

        myShell->shell_pgid = getpid();
        if (setpgid (myShell->shell_pgid, myShell->shell_pgid) < 0)
        {
            exit(1);
        }
        tcsetpgrp (myShell->shell_terminal, myShell->shell_pgid);
        tcgetattr (myShell->shell_terminal, &myShell->shell_tmodes);

    }
    return myShell;
}

void init_process(char **cmd, pid_t pgid, shell *newShell, int foreground) {
    pid_t pid = getpid();
    setpgid(pid, pgid);
    if (foreground) {
        tcsetpgrp(newShell->shell_terminal, pgid);
    }
    // reset signals to default
    signal (SIGINT, SIG_DFL);
    signal (SIGQUIT, SIG_DFL);
    signal (SIGTSTP, SIG_DFL);
    signal (SIGTTIN, SIG_DFL);
    signal (SIGTTOU, SIG_DFL);
    // exec the process
    execvp(cmd[0], cmd);
    exit(EXIT_FAILURE);
}

int job_is_completed (job *j) {
    process *p;

    for (p = j->first_process; p; p = p->next)
        if (!p->completed) {
            return 0;
        }
    return 1;
}

int job_is_stopped (job *j) {
  process *p;

  for (p = j->first_process; p; p = p->next)
    if (!p->completed && !p->stopped)
      return 0;
  return 1;
}

void wait_for_job (job *j) {
    int status;

    do {
        waitpid (-1, &status, WUNTRACED);
    } while (!job_is_stopped (j) && !job_is_completed (j));
}

void put_job_in_foreground (job *j, int cont, shell *newShell) {
    /* Put the job into the foreground.  */
    tcsetpgrp (newShell->shell_terminal, j->pgid);

    /* Send the job a continue signal, if necessary.  */
    if (cont)
        {
        if (kill (- j->pgid, SIGCONT) < 0)
            perror ("kill (SIGCONT)");
        }

    /* Wait for it to report.  */
    wait_for_job (j);

    /* Put the shell back in the foreground.  */
    tcsetpgrp (newShell->shell_terminal, newShell->shell_pgid);
}

void put_job_in_background (job *j, int cont) {
    /* Send the job a continue signal, if necessary.  */
    if (cont)
        if (kill (-j->pgid, SIGCONT) < 0)
            perror ("kill (SIGCONT)");

    // print information about background job
    fprintf (stdout, "[%d] %ld %s\n", j->jnum, (long)j->pgid, j->command);
}

void init_job(char **cmd, job *j, int foreground, shell *newShell) {
    process *p = (process*) malloc(sizeof(process));
    memset(p, 0, sizeof(process));

    // fork process
    pid_t pid = fork();
    if (pid == 0) {
        init_process(cmd, j->pgid, newShell, foreground);
    } else if (pid > 0) {
        p->pid = pid;
        if (newShell->shell_is_interactive) {
            if (!j->pgid) {
                j->pgid = pid;
            }
            setpgid(pid, j->pgid);
        }
    }
    // determine foreground or background
    if (foreground) {
        put_job_in_foreground (j, 0, newShell);
    } else {
        put_job_in_background (j, 0);
    }
    free(p);
}

char *get_prompt(const char *env) {
    // set default prompt
    char *defaultPrompt = (char *) malloc(sizeof(char) * 8);
    strcpy(defaultPrompt, "shell> ");
    char *userEnv = getenv(env);

    // get user prompt if available
    if (userEnv) {
        char *newPrompt = (char *) malloc(sizeof(userEnv));
        strcpy(newPrompt, userEnv);
        free(defaultPrompt);
        return newPrompt;
    }
    return defaultPrompt;
}

int change_dir(char **dir) {
    char *env = dir[1];
    int newDir = -1;

    // change directory if exists
    if (!env) {
        uid_t uid = getuid();
        struct passwd *pwuid = getpwuid(uid);
        newDir = chdir(pwuid->pw_name);
    } else {
        newDir = chdir(env);
    }
    return newDir;
}

char **cmd_parse(char const *line) {
    const long arg_max = sysconf(_SC_ARG_MAX);
    char *tmp = strdup(line);
    char *save = NULL;
    if (!tmp)
    {
        fprintf(stderr, "strdup returned NULL! This should never happen");
        abort();
    }
    char **rval = (char **)calloc(arg_max, sizeof(char*));
    char *tok = strtok_r(tmp, " ", &save);

    for (int i = 0; tok && i < arg_max; i++)
    {
        //We can save a malloc call here only because strtok_r is chopping up the
        //string for us and we previously copied it with strdup!
        //Read the following article below for more details
        //https://www.ibm.com/docs/en/zos/2.1.0?topic=functions-strtok-r-split-string-into-tokens
        rval[i] = tok;
        tok = strtok_r(NULL, " ", &save);
    }
    return rval;
}

void cmd_free(char ** line) {
    char *tmp = *line;
    free(tmp);
    free((void *)line);
}

char *trim_white(char *line) {
    int start = 0;
    int end = strlen(line) - 1;

    // move start and end together to eliminate spaces
    while (line[start] == ' ')
    {
        start++;
    }
    while (line[end] == ' ')
    {
        end--;
    }
    line[end + 1] = '\0';
    
    // if there are extra spaces in command, ignore them
    if (strlen(line) > 2){
        memmove(line, line + start, end - start + 2);
    }
    return line;
}

int go(int argc, char **argv) {
    // print version
    int opt;
    while ((opt = getopt(argc, argv, "v")) != -1)
    {
        switch(opt)
        {
            case 'v':
                printf("%d.%d\n", lab_VERSION_MAJOR, lab_VERSION_MINOR);
                exit(0);
        }
    }

    int jnum = 0; // to keep track of background job numbers
    char *line;
    char *prompt = get_prompt("MY_PROMPT");
    shell *newShell = init_shell();
    // job *first_job = NULL;
    using_history();
    while ((line=readline(prompt))) {
        line = trim_white(line); // trim whitespace
        if (strcmp(line, "") == 0) { // if user just presses enter or spaces, don't SEGV
            free(line);
            continue;

        } else if (line == NULL || strcmp(line, "exit") == 0) { // exit built-in
            printf("\nExiting shell... See ya!\n\n");
            rl_callback_handler_remove();
            free(line);
            exit(0);

        } else if ((line[0] == 'c') && (line[1] == 'd')) { // change directory built-in
            char **dir = cmd_parse(line);
            change_dir(dir);
            cmd_free(dir);
            add_history(line);
            free(line);

        } else if (strcmp(line, "history") == 0) { // history built-in
            int i = history_base;
            printf("\n");
            while (history_get(i)) {
                printf("%s\n", history_get(i)->line);
                i++;
            }
            printf("\n");
            add_history(line);
            free(line);

        } else {
            char **cmd = cmd_parse(line);
            job *j = (job*) malloc(sizeof(job));
            memset(j, 0, sizeof(job));
            j->command = line;
            
            // check for ampersand to run job in background
            int i = 1;
            int amp = 0;
            while (line[i] != '\0') {
                if (line[i] == '&') {
                    amp = 1;
                }
                i++;
            }
            if (amp == 1) { // if we have an ampersand...
                int n = 0;
                while (line[n] != '&') {
                    n++;
                }
                // ... add job to background
                jnum++;
                j->jnum = jnum;
                // get rid of ampersand to run background process successfully
                char *newLine = rl_copy_text(0,n);
                char **newcmd = cmd_parse(newLine);
                init_job(newcmd, j, 0, newShell);
                add_history(newLine);
                free(newLine);
                cmd_free(newcmd);
            } else { // ... otherwise, run job in foreground
                init_job(cmd, j, 1, newShell);
                add_history(line);
            }
            free(line);
            free(j);
            cmd_free(cmd);
        }
    }
    free(prompt);
    free(newShell);
    clear_history();

    return 0;
}