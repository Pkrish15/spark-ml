# spark-ml
1) This project mainly focuses on realistic usecases and also to showcase the capability of SparkMLLIB for realtime data analysis
# Datasets used
2) https://www.transtats.bts.gov/DL_SelectFields.asp  - For Flight Delay Prediction usecase.
3) http://archive.ics.uci.edu/ml/index.php - For Optical Character Recognization.
4) https://archive.ics.uci.edu/ml/datasets/Breast+Cancer+Wisconsin+(Prognostic) - For Breast Cancer Prognostic Prediction.
5) https://archive.ics.uci.edu/ml/machine-learning-databases/breast-cancer-wisconsin/wdbc.names - For Breast cancer diagnostic Prediction.
# Use Cases Addressed
a) Flight Delay Prediction <br>
b) Optical Character Recognization <br>
c) Breast Cancer Prognostic and Diagnostic Prediction.<br>
d) CreditCard Fraud Detection (Working on it - Facing some minor exceptions) <br>
e) Basket Analysis for Groceries (Working on it - Facing some minor exceptions) <br>

# Input Data
6) All the input data have been extracted from the above links and placed in our project folder.
# Pre-requisities to run these examples
7) https://github.com/Pkrish15/ans-loc-spk - Please follow the instructions given in the Readme file. This Playbook script will install Local spark on your desktop.
# Spark-Master URL
8) Run the Spark Master URL from your Local Desktop $ /opt/spark/spark 2.3 XXX/sbin - ./start-master.sh
9) Hit the Localhost url http://localhost:8080/ and Ensure the spark master is running.

# Spark-Submit
10) Run the Maven Commands: $ spark-ml/ mvn clean install package -e 
11) Goto your local spark directory $ /opt/spark/spark 2.3.XXX/bin - ./spark-submit --master spark://localhost:7077 \ --class com.redhat.gpte.JavaFPGrowthExample \ /tmp/input.txt \ /target/spark-ml.jar
12) https://spark.apache.org/docs/latest/submitting-applications.html  -- More Optional Deployments is given in this Link.

# Deploy in OpenShift
13) Connect to the Spark Master http://spark-master-webui-tk.apps.dev39.openshift.opentlc.com/
14) oc rsh <slave/worker/masterpod>
15) oc rsync /root/spark-ml/target/spark-ml.jar /tmp/spark-ml.jar
16) spark-submit from /opt/spark/bin/ - spark-submit --master spark://spark://10.1.6.124:7077 \ -- class com.redhat.gpte.JavaFPGrowthExample \ /tmp/input.txt \ /tmp/spark-ml.jar








