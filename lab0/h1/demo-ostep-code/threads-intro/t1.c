#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "common.h"
#include "common_threads.h"

volatile int counter = 0; // shared global variable

void *mythread(void *arg) {
    int i; // stack (private per thread) 
    printf("%s: begin\n", (char *)arg);
    for (i = 0; i < 10000000; i++) {
	   counter = counter + 1; // shared: only one
    }
    printf("%s: done\n", (char *)arg);
    return NULL;
}
                                                                             
int main(int argc, char *argv[]) {                    
    pthread_t p1, p2;
    printf("main: begin [counter = %d]\n", counter);
    Pthread_create(&p1, NULL, mythread, "A"); 
    Pthread_create(&p2, NULL, mythread, "B");
    // join waits for the threads to finish
    Pthread_join(p1, NULL); 
    Pthread_join(p2, NULL); 
    printf("main: done with [counter = %d]\n", counter);
    return 0;
}

