# spark-toolbox
A toolbox for minimal spark submission and scheduling
## Scheduler
The scheduler is still wip, every submission should
* Clean all pods under namespace **spark**
* At least gaps 1 second


## Submitter
Example:
```bash
./target/debug/spark-submitter \
    --n-workload 4 \
    --scheduler-name spark-sched \
    --path /home/xyji/spark/bin/spark-submit \
    --master k8s://https://10.2.7.200:6443 \
    --image xinyouji/pyspark-image:latest \
    --planner fair \
    --pvc-claim-name nfs-pvc-spark \
    --prog local:///opt/spark/examples/src/main/python/wordcount.py \
    --args /mnt/input.txt
```
