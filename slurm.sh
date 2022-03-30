#!/bin/bash -x
if [ "x$SLURM_NPROCS" = "x" ]
then
    if [ "x$SLURM_NTASKS_PER_NODE" = "x"]
    then
        SLURM_NTASKS_PER_NODE=1
    fi
    SLURM_NPROCS=`expr $SLURM_JOB_NUM_NODES \* $SLURM_NTASKS_PER_NODE`
else
    if ["x$SLURM_NTASKS_PER_NODE",= " x"]
    then
        SLURM_NTASKS_PER_NODE=`expr $SLURM_NPROCS / $SLURM_JOB_NUM_NODES`
    fi
fi

srun hostname -s | sort -u > /tmp/hosts.$SLURM_JOB_ID
awk " { print \$0 \"-ib slots=$SLURM_NTASKS_PER_NODE\"; }"\
/tmp/hosts.$SLURM_JOB_ID >/tmp/tmp.$SLURM_JOB_ID
mv /tmp/tmp.$SLURM_JOB_ID /tmp/hosts.$SLURM_JOB_ID

module load xl_r spectrum-mpi cuda/11.2

taskset --cpu-list 0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76,80,84,88,92,96,100,104,108,112,116,120,124 mpirun --bind-to core --report-bindings -hostfile /tmp/hosts.$SLURM_JOB_ID -np $SLURM_NPROCS /gpfs/u/home/PCPB/PCPBqdnl/scratch/parallelhw5/main /mnt/c/users/danie/dropbox/parallel/Assignment5/test.txt
rm /tmp/hosts.$SLURM_JOB_ID
