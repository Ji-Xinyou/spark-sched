#!/bin/bash

fn=5s2c

for i in {1..10}; do
    ./f.sh > "../logs/$fn/la_f_output${i}.log"
done

for i in {1..10}; do
    ./p.sh > "../logs/$fn/la_p_output${i}.log"
done

# for i in {1..10}; do
#     ./sf.sh > "../logs/$fn/sf_output${i}.log"
# done

# for i in {1..10}; do
#     ./sp.sh > "../logs/$fn/sp_output${i}.log"
# done
