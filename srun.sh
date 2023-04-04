./target/debug/spark-submitter \
	--path /home/xyji/spark/bin/spark-submit \
	--master k8s://https://10.2.7.200:6443 \
	--image xinyouji/pyspark-image:latest \
	--planner workload \
	--scheduler-name spark-sched \
	--pvc-claim-name nfs-pvc-spark \
	--tags storage \
	--progs "local:///opt/spark/examples/src/main/python/wordcount.py /mnt/input.txt" \
	--tags storage \
	--progs "local:///opt/spark/examples/src/main/python/wordcount.py /mnt/input.txt" \
	--tags storage \
	--progs "local:///opt/spark/examples/src/main/python/wordcount.py /mnt/input.txt" \
	--tags compute \
	--progs "local:///opt/spark/examples/src/main/python/pi.py 500" \
	--tags compute \
	--progs "local:///opt/spark/examples/src/main/python/pi.py 500" \
	--tags compute \
	--progs "local:///opt/spark/examples/src/main/python/pi.py 500" \