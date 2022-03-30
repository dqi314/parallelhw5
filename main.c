#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include "clockcycle.h"

#define BLOCKS 32
#define CLOCK_RATE 512000000
#define DUMMY_CHAR '1'
#define K 1024
#define BLOCK_SIZE_COUNT 8

int taskid, numtasks;
long long block_sizes[] = { 128 * K, 256 * K, 512 * K, 1024 * K, 2048 * K, 4096 * K, 8192 * K, 2 * 8192 * K };
long long BLOCK_SIZE;
long long start_cycles, end_cycles, cycles_passed;


/**
 * @brief Prints how much time since start_cycles was set
 * as well as bandwidth in GB/s
 */
void print_time() {
    end_cycles = clock_now();
    cycles_passed = ((double)(end_cycles - start_cycles)); 
    printf("%lld cycles elapsed\n", cycles_passed);
    printf("> Bandwidth = %f MB/s\n", cycles_passed / ((double)CLOCK_RATE) * BLOCKS * BLOCK_SIZE / 1e6);
}

/**
 * @brief Perform the write test
 * @param file Path to file to write to
 */
void write_test(char * file) {
    MPI_Barrier(MPI_COMM_WORLD); // Ensure all ranks here

    // Open file in CREATE or WRITE_ONLY mode
    MPI_File fh;
    MPI_Status status;
    MPI_File_open(MPI_COMM_WORLD, file, MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

    // Create a temporary buffer of all 1s
    char * buf = malloc(sizeof(char) * BLOCK_SIZE);
    for (unsigned long i = 0; i < BLOCK_SIZE; i++)
        buf[i] = DUMMY_CHAR;

    // Start test
    start_cycles = clock_now();
    for (unsigned long i = 0; i < BLOCKS; i++)
        MPI_File_write_at(fh,
            taskid * BLOCK_SIZE * BLOCKS, // Offset by (BLOCKS * BLOCK_SIZE) per rank
            buf, BLOCK_SIZE, MPI_CHAR, &status);
        MPI_Barrier(MPI_COMM_WORLD);
    MPI_File_close(&fh);
    
    if (taskid == 0)
        print_time();
    free(buf);
}

/**
 * @brief Perform the read test
 * @param file Path to file to read from
 */
void read_test(char * file) {
    MPI_Barrier(MPI_COMM_WORLD); // Ensure all ranks here

    // Open file in READ_ONLY mode
    MPI_File fh;
    MPI_Status status;
    MPI_File_open(MPI_COMM_WORLD, file, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);

    // Create a temporary buffer to write to
    char * buf = malloc(sizeof(char) * BLOCK_SIZE * BLOCKS);
    
    start_cycles = clock_now();
    for (unsigned long i = 0; i < BLOCKS; i++)
        MPI_File_read_at(fh,
            taskid * BLOCK_SIZE * BLOCKS, // Offset by (BLOCKS * BLOCK_SIZE) per rank
            buf + i * BLOCK_SIZE, BLOCK_SIZE, // Write everything to buffer
            MPI_CHAR, &status);
        MPI_Barrier(MPI_COMM_WORLD);
    MPI_File_close(&fh);
    
    if (taskid == 0)
        print_time();

    // Sanity check
    for (unsigned long i = 0; i < BLOCKS * BLOCK_SIZE; i++) {
        if (buf[i] != DUMMY_CHAR) {
            printf("[Error] Rank %d failed to read/write at relative positon %lu, got char 0x%02x instead!\n", taskid, i, buf[i]);
            break;
        }
    }
    free(buf);
}

int main(int argc, char **argv) {
    // Usage:
    // ./main <directory to file>
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

    if (taskid == 0)
        printf("[Run %d Tasks]\n", numtasks);

    // Perform tests
    for (unsigned int i = 0; i < BLOCK_SIZE_COUNT; i++) {
        if (taskid == 0)
            printf("\nPerforming test with block size %lld\n--------------\n", block_sizes[i]);
        BLOCK_SIZE = block_sizes[i];
        write_test(argv[1]);
        read_test(argv[1]);

        if (taskid == 0)
            MPI_File_delete(argv[1], MPI_INFO_NULL);
    }

    MPI_Finalize();
}
